//! Shopping Sessions pipeline. See rules.md.
//!
//! Phases:
//!   0. products, campaigns, manifest; links -> union-find -> person names.
//!   1. parse event and click chunks in parallel. Event copies are scattered by
//!      event_id hash; valid clicks are resolved to a person and scattered by person.
//!   2. per event_id partition: dedup (smallest ingest_ts), keep surviving kept
//!      copies, resolve person, scatter by person.
//!   3. per person partition: group, sort, sessionize, attribute, aggregate.
//!   4. write Parquet.

mod alloc;
mod buf;
mod csv;
mod out;
mod pq;
mod table;
mod ts;

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use buf::{Pool, Scatter};
use memmap2::Mmap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::{AcqRel, Acquire, Relaxed, Release}};
use table::{uhash, AtomicMap, IdIndex};
use buf::ZeroVec;
use xxhash_rust::xxh3::xxh3_64;

pub const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;

const CHUNK: usize = 4 << 20;
const PREFETCH: usize = 16;

const LINKED: u64 = 1 << 63;

/// Large heap blocks get transparent huge pages (see alloc.rs).
#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

// ---------------------------------------------------------------- hashing

#[derive(Default, Clone, Copy)]
pub struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = xxh3_64(bytes) ^ self.0.rotate_left(29);
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
pub type IdBuild = BuildHasherDefault<IdHasher>;

#[inline]
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}


// ---------------------------------------------------------------- byte store

/// All input files, memory-mapped. A `Ptr` (u64) names a byte range in one of
/// them: file (12 bits) | offset (36 bits) | len (16 bits). File 4095 means
/// the overflow arena, with the offset as an index.
pub struct Store {
    maps: Vec<Mmap>,
    overflow: Mutex<Vec<&'static [u8]>>,
    /// Per file: its first byte's global offset (files laid end to end).
    gbase: Vec<u64>,
}

const OVERFLOW_FILE: u64 = 4095;

impl Store {
    #[inline]
    pub fn ptr(&self, file: usize, base: *const u8, s: &[u8]) -> u64 {
        let off = s.as_ptr() as usize - base as usize;
        let len = s.len();
        if len < 0xFFFF && off < (1usize << 36) {
            ((file as u64) << 52) | ((off as u64) << 16) | len as u64
        } else {
            let leaked: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
            let mut v = self.overflow.lock().unwrap();
            v.push(leaked);
            (OVERFLOW_FILE << 52) | (((v.len() - 1) as u64) << 16)
        }
    }

    /// A name for `get_name`: the field value (ended by `,` or `"`) at global
    /// offset `g`. Low 16 bits 0xFFFF never occur in a `Ptr`.
    #[inline]
    pub fn field_name(g: u64) -> u64 {
        g << 16 | 0xFFFF
    }

    /// The bytes of a `Ptr` or of a `field_name`.
    pub fn get_name(&self, n: u64) -> &[u8] {
        if n & 0xFFFF != 0xFFFF {
            return self.get(n);
        }
        let g = n >> 16;
        let fi = self.gbase.partition_point(|&b| b <= g) - 1;
        let m = &self.maps[fi];
        let off = (g - self.gbase[fi]) as usize;
        let len = memchr::memchr2(b',', b'"', &m[off..]).unwrap_or(m.len() - off);
        &m[off..off + len]
    }

    #[inline]
    pub fn get(&self, p: u64) -> &[u8] {
        let file = p >> 52;
        let off = ((p >> 16) & ((1u64 << 36) - 1)) as usize;
        if file == OVERFLOW_FILE {
            let v = self.overflow.lock().unwrap();
            return v[off];
        }
        let len = (p & 0xFFFF) as usize;
        &self.maps[file as usize][off..off + len]
    }
}

// ---------------------------------------------------------------- context

pub struct Ctx {
    ws: i64,
    we: i64,
    /// Base for the u32 relative timestamps: window_start - 7 days.
    base: i64,
    /// product_id -> category rank (remapped)
    products: IdIndex,
    unknown_cat: u16,
    /// category names sorted byte-wise; index = rank
    pub categories: Vec<Vec<u8>>,
    campaigns: IdIndex,
    pub campaign_ids: Vec<Vec<u8>>,
    pub channels: Vec<Vec<u8>>,
    /// user_id hash -> component (root slot), for every ID named in a valid link line
    links: AtomicMap,
    /// root slot -> Ptr of its name
    comp_name: ZeroVec<AtomicU64>,
    /// Set (Release) once `links` and `comp_name` are final; `person` only after.
    linked: AtomicBool,
    qshift: u32,
    pub day0: i64,
    pub ndays: usize,
}

impl Ctx {
    #[inline]
    fn person(&self, uh: u64) -> u64 {
        match self.links.get(uh) {
            Some(c) => LINKED | c as u64,
            None => uh & !LINKED,
        }
    }
    #[inline]
    fn qpart(&self, pkey: u64) -> usize {
        (mix64(pkey) >> self.qshift) as usize
    }
    #[inline]
    fn category(&self, product: &[u8], room: bool) -> u16 {
        match self.products.get_fast(product, room) {
            Some(c) => c as u16, // remapped to the category
            None => self.unknown_cat,
        }
    }
}

// ---------------------------------------------------------------- records

const M40: u64 = (1 << 40) - 1;
const M48: u64 = (1 << 48) - 1;

/// One event copy, for dedup (pass 1 -> 2): a 104-bit event_id key, ingest_ts
/// in seconds since 0000-01-01 (40 bits), and the address of its `P` record
/// (48 bits; null for an invalid copy).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct K {
    w0: u64, // key bits 0..64, never 0; the top bits pick the partition
    w1: u64, // key bits 64..88 << 40 | ingest
    w2: u64, // key bits 88..104 << 48 | P address
}

impl K {
    #[inline(always)]
    fn new(h: u128, ingest: u64) -> K {
        K { w0: (h as u64).max(1), w1: (((h >> 64) as u64) << 40) | ingest, w2: ((h >> 88) as u64) << 48 }
    }
    #[inline(always)]
    fn same(&self, o: &K) -> bool {
        self.w0 == o.w0 && (self.w1 ^ o.w1) >> 40 == 0 && (self.w2 ^ o.w2) >> 48 == 0
    }
    #[inline(always)]
    fn ingest(&self) -> u64 {
        self.w1 & M40
    }
    /// Marks this copy's surviving record (if any) as a dedup loser.
    #[inline(always)]
    fn kill(&self) {
        let p = (self.w2 & M48) as *mut P;
        if !p.is_null() {
            unsafe { (*p).dead = 1 };
        }
    }
}

/// ingest_ts (always F1 per the rules) as seconds since 0000-01-01, which
/// orders every F1 value exactly; unparseable sorts last.
#[inline(always)]
fn ingest_secs(s: &[u8]) -> u64 {
    let secs = match ts::f1_secs(s) {
        Some(secs) => secs,
        None => match ts::parse(s) {
            Some(ms) => ms.div_euclid(1000),
            None => return M40,
        },
    };
    (secs + 62_167_219_200).clamp(0, M40 as i64 - 1) as u64
}

/// A valid event copy, by person (pass 1 -> 3).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct P {
    pkey: u64,
    gp: u64, // global offset of its user_id (40 bits) | price << 40
    ts: u32,
    cat: u16,
    dead: u16, // set by dedup for a losing copy
}

impl P {
    #[inline(always)]
    fn price(&self) -> u32 {
        (self.gp >> 40) as u32
    }
}

/// A valid click.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct C1 {
    pkey: u64,
    cptr: u64,
    ts: u32,
    camp: u32,
}

pub struct URow {
    pub name: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top: u32, // u32::MAX = null
}

/// Adds the elapsed time to a counter when dropped.
struct Timer(*mut f64, Instant);
impl Drop for Timer {
    fn drop(&mut self) {
        unsafe { *self.0 += self.1.elapsed().as_secs_f64() };
    }
}

// ---------------------------------------------------------------- parallel helper

fn par_run<S: Send, F: Fn(&mut S, usize) + Sync>(states: Vec<S>, ntasks: usize, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = states
            .into_iter()
            .map(|mut st| {
                let next = &next;
                let f = &f;
                scope.spawn(move || {
                    loop {
                        let t = next.fetch_add(1, Ordering::Relaxed);
                        if t >= ntasks {
                            break;
                        }
                        f(&mut st, t);
                    }
                    st
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// `par_run`, then `fin(worker index, state)` on each worker as soon as it
/// runs out of tasks (overlapping the others' last tasks).
fn par_run_fin<S: Send, F: Fn(&mut S, usize) + Sync, G: Fn(usize, &mut S) + Sync>(states: Vec<S>, ntasks: usize, f: F, fin: G) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = states
            .into_iter()
            .enumerate()
            .map(|(w, mut st)| {
                let (next, f, fin) = (&next, &f, &fin);
                scope.spawn(move || {
                    loop {
                        let t = next.fetch_add(1, Ordering::Relaxed);
                        if t >= ntasks {
                            break;
                        }
                        f(&mut st, t);
                    }
                    fin(w, &mut st);
                    st
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> (Mmap, usize) {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        // memmap2 can't map an empty file; map an anonymous empty region instead.
        return (memmap2::MmapOptions::new().len(1).map_anon().unwrap().make_read_only().unwrap(), 0);
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    (m, len)
}

/// Data byte ranges of a file (after its header line), split near `CHUNK` at newlines.
fn chunks_of(data: &[u8]) -> Vec<(usize, usize)> {
    let start = match memchr::memchr(b'\n', data) {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let mut v = Vec::new();
    let mut s = start;
    while s < data.len() {
        let mut e = (s + CHUNK).min(data.len());
        if e < data.len() {
            e = match memchr::memchr(b'\n', &data[e..]) {
                Some(i) => e + i + 1,
                None => data.len(),
            };
        }
        v.push((s, e));
        s = e;
    }
    v
}

#[inline]
fn for_lines(data: &[u8], mut f: impl FnMut(&[u8])) {
    let mut pos = 0;
    let n = data.len();
    while pos < n {
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => n,
        };
        f(&data[pos..e]);
        pos = e + 1;
    }
}

/// Header-skipping read of a small clean CSV, every line through the dialect parser.
fn read_small<const N: usize>(path: &Path, mut f: impl FnMut(&[&[u8]; N])) {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let start = match memchr::memchr(b'\n', &bytes) {
        Some(i) => i + 1,
        None => return,
    };
    for_lines(&bytes[start..], |line| {
        let mut fs: [&[u8]; N] = [&[]; N];
        if csv::split::<N>(line, &mut fs) {
            f(&fs);
        }
    });
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).unwrap_or_else(|| panic!("manifest: no {key}"));
    let rest = &text[at + key.len() + 2..];
    let colon = rest.find(':').unwrap();
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    ts::parse(rest[..q2].as_bytes()).unwrap_or_else(|| panic!("manifest: bad {key}"))
}

/// (minor faults, user+sys CPU seconds) of this process.
fn proc_stat() -> (u64, f64) {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let f: Vec<&str> = s[s.rfind(')')? + 2..].split(' ').collect();
            let ticks: f64 = f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?;
            Some((f.get(7)?.parse().ok()?, ticks / 100.0))
        })
        .unwrap_or((0, 0.0))
}

/// Distinct keys in first-seen order, and for each line its key's position.
fn dedup_keys(keys: &mut Vec<Vec<u8>>, seen: &mut hashbrown::HashMap<Vec<u8>, u32, IdBuild>, k: &[u8]) -> (u32, bool) {
    if let Some(&i) = seen.get(k) {
        return (i, false);
    }
    let i = keys.len() as u32;
    keys.push(k.to_vec());
    seen.insert(k.to_vec(), i);
    (i, true)
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .min(32);
    let lap = |what: &str| {
        let (flt, cpu) = proc_stat();
        eprintln!("[{:>7.3}s] {what}  (cpu {cpu:.2}s, minflt {flt})", t0.elapsed().as_secs_f64())
    };

    // ---- manifest, products, campaigns
    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let base = ws - WEEK;
    assert!(we - base < u32::MAX as i64, "window too long");

    let mut product_keys = Vec::new();
    let mut product_cat_raw: Vec<u32> = Vec::new();
    let mut cat_keys: Vec<Vec<u8>> = Vec::new();
    {
        let mut seen_p = hashbrown::HashMap::default();
        let mut seen_c = hashbrown::HashMap::default();
        read_small::<2>(&data_dir.join("products.csv"), |f| {
            let (_, new) = dedup_keys(&mut product_keys, &mut seen_p, f[0]);
            let (c, _) = dedup_keys(&mut cat_keys, &mut seen_c, f[1]);
            if new {
                product_cat_raw.push(c);
            }
        });
        // "unknown" is a name no category in the file uses.
        dedup_keys(&mut cat_keys, &mut seen_c, b"unknown");
    }
    let mut order: Vec<u32> = (0..cat_keys.len() as u32).collect();
    order.sort_by(|&a, &b| cat_keys[a as usize].cmp(&cat_keys[b as usize]));
    assert!(order.len() < u16::MAX as usize, "too many categories");
    let mut rank = vec![0u16; order.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u16;
    }
    let categories: Vec<Vec<u8>> = order.iter().map(|&c| cat_keys[c as usize].clone()).collect();
    let product_cat: Vec<u16> = product_cat_raw.iter().map(|&c| rank[c as usize]).collect();
    let unknown_cat = rank[cat_keys.iter().position(|k| k == b"unknown").unwrap()];
    let mut products = IdIndex::build(&product_keys, b"p");
    products.remap(&product_cat.iter().map(|&c| c as u32).collect::<Vec<_>>());

    let mut campaign_ids = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    {
        let mut seen = hashbrown::HashMap::default();
        read_small::<2>(&data_dir.join("campaigns.csv"), |f| {
            if dedup_keys(&mut campaign_ids, &mut seen, f[0]).1 {
                channels.push(f[1].to_vec());
            }
        });
    }
    let campaigns = IdIndex::build(&campaign_ids, b"c_");
    let ncamp = campaign_ids.len();

    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // ---- map every CSV
    let mut paths: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, sub) in [(0u8, "links"), (1, "events"), (2, "clicks")] {
        for p in list_csv(&data_dir.join(sub)) {
            paths.push((kind, p));
        }
    }
    assert!(paths.len() < OVERFLOW_FILE as usize, "too many files");
    let mut lens = Vec::new();
    let maps: Vec<Mmap> = paths
        .iter()
        .map(|(_, p)| {
            let (m, len) = map_file(p);
            lens.push(len);
            m
        })
        .collect();
    let mut gbase = Vec::with_capacity(lens.len());
    let mut g = 0u64;
    for &l in &lens {
        gbase.push(g);
        g += l as u64;
    }
    assert!(g < M40, "input too large");
    let store = Store { maps, overflow: Mutex::new(Vec::new()), gbase };
    let mut link_chunks = Vec::new();
    let mut main_chunks = Vec::new();
    let mut bytes = [0usize; 3];
    for (fi, (kind, _)) in paths.iter().enumerate() {
        bytes[*kind as usize] += lens[fi];
        for (s, e) in chunks_of(&store.maps[fi][..lens[fi]]) {
            if *kind == 0 {
                link_chunks.push((fi, s, e, 0u8));
            } else {
                main_chunks.push((fi, s, e, *kind));
            }
        }
    }
    // Events first, then clicks (they need the links); largest first within each.
    main_chunks.sort_by_key(|&(_, s, e, kind)| (kind, std::cmp::Reverse(e - s)));
    let n_ev_tasks = main_chunks.iter().filter(|c| c.3 == 1).count();
    lap("setup");

    // ---- links: tables sized by the number of link lines, which bounds the
    // number of valid pairs. The work runs inside pass 1 (see the pool there).
    type LinkPair = (u64, u64, u64, u64); // (ha, pa, hb, pb)
    let nlines: usize = link_chunks.iter().map(|&(fi, s, e, _)| store.maps[fi][s..e].iter().filter(|&&b| b == b'\n').count() + 1).sum();
    // Concurrent union-find over slot indices of a concurrent ID table.
    // parent[x] == 0 means x is a root; otherwise parent[x] - 1 is its parent.
    let idmap = AtomicMap::new(nlines * 2 + 16);
    let cap = idmap.capacity();
    let parent: ZeroVec<AtomicU32> = ZeroVec::new(cap);
    let idptr: ZeroVec<AtomicU64> = ZeroVec::new(cap);
    let comp_name: ZeroVec<AtomicU64> = ZeroVec::new(cap);
    #[inline]
    fn find(p: &[AtomicU32], mut x: usize) -> usize {
        loop {
            let px = p[x].load(Relaxed);
            if px == 0 {
                return x;
            }
            let up = px as usize - 1;
            let gp = p[up].load(Relaxed);
            if gp == 0 {
                return up;
            }
            p[x].store(gp, Relaxed); // path halving: gp - 1 is an ancestor of x
            x = gp as usize - 1;
        }
    }
    const SLOT_TASK: usize = 1 << 16;

    // Partition counts: about 8k event copies per event_id partition, capped.
    let est_events = bytes[1] / 70 + 1024;
    let est_clicks = bytes[2] / 45 + 1024;
    // At most 2^10 + 2^9 write streams per thread: their staging buffers
    // (192 B each) must stay well inside a hyperthread's share of L2. Pass 2's
    // filter and pass 3's counting sorts cope with the larger partitions.
    let pb: u32 = (usize::BITS - 1 - (est_events / 8000).max(16).leading_zeros()).min(10);
    let qb: u32 = (pb - 1).min(9);
    let (np, nq) = (1usize << pb, 1usize << qb);
    let ctx = Ctx {
        ws,
        we,
        base,
        products,
        unknown_cat,
        categories,
        campaigns,
        campaign_ids,
        channels,
        links: idmap,
        comp_name,
        linked: AtomicBool::new(false),
        qshift: 64 - qb,
        day0,
        ndays,
    };

    // ---- pass 1: events and clicks
    // Valid event copies go straight to person partitions (P); every copy also
    // leaves a K record in its event_id partition, pointing at its P record.
    let pool = Pool::new(est_events * 2 * 48 + est_clicks * 2 * 24 + nthreads * (np + 2 * nq) * buf::BLK * 2);
    const BATCH: usize = 32;
    struct S1 {
        k: Scatter<K>,
        ev: Scatter<P>,
        cl: Scatter<C1>,
        pend: Vec<(u64, K, P)>,
        pending: Vec<(u64, C1)>,
        buf: Vec<u64>,
        pairs: Vec<LinkPair>,
        cpu: [f64; 3],
    }
    let s1: Vec<S1> = (0..nthreads)
        .map(|_| S1 {
            k: Scatter::new(np),
            ev: Scatter::new(nq),
            cl: Scatter::new(nq),
            pend: Vec::with_capacity(BATCH),
            pending: Vec::new(),
            buf: Vec::new(),
            pairs: Vec::new(),
            cpu: [0.0; 3],
        })
        .collect();
    /// Resolves the people of a batch of valid copies (their link slots were
    /// prefetched when parsed) and scatters them. Until the links are done the
    /// copies wait in `pend` (then many at once, prefetched again here).
    #[inline(always)]
    fn flush(ctx: &Ctx, pool: &Pool, pb: u32, pend: &mut Vec<(u64, K, P)>, k: &mut Scatter<K>, ev: &mut Scatter<P>) {
        if !ctx.linked.load(Acquire) {
            return;
        }
        let n = pend.len();
        for i in 0..n {
            if n > BATCH && i + PREFETCH < n {
                ctx.links.prefetch(pend[i + PREFETCH].0);
            }
            let (uh, mut kr, mut pr) = pend[i];
            pr.pkey = ctx.person(uh);
            let at = ev.push(pool, ctx.qpart(pr.pkey), pr);
            kr.w2 |= at as u64;
            k.push(pool, (kr.w0 >> (64 - pb)) as usize, kr);
        }
        pend.clear();
    }
    let chunk_task = |st: &mut S1, t: usize| {
        let (fi, s, e, kind) = main_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        let ctx = &ctx;
        let pool = &pool;
        // At least 8 readable bytes end at a field's end (the header precedes all data).
        let room = |f: &[u8]| f.as_ptr() as usize + f.len() >= basep as usize + 8;
        let tk = Instant::now();
        let _timer = Timer(&mut st.cpu[kind as usize] as *mut f64, tk);
        if kind == 1 {
            let gb = (store.gbase[fi] as usize).wrapping_sub(basep as usize);
            let S1 { k, ev, pend, buf, .. } = st;
            csv::scan::<7>(&m[s..e], buf, |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let kr = K::new(table::ekey(eid), ingest_secs(f[3]));
                'v: {
                    let uid = f[1];
                    if uid.is_empty() {
                        break 'v;
                    }
                    let t = match ts::parse(f[2]) {
                        Some(t) if t >= ctx.ws && t < ctx.we => t,
                        _ => break 'v,
                    };
                    let purchase = match csv::event_type(f[4], room(f[4])) {
                        Some(p) => p,
                        None => break 'v,
                    };
                    if f[5].is_empty() {
                        break 'v;
                    }
                    // The category only matters for purchases (revenue, top_category).
                    let (price, cat) = if purchase {
                        match csv::price_fast(f[6], room(f[6])) {
                            Some(p) => (p, ctx.category(f[5], room(f[5]))),
                            None => break 'v,
                        }
                    } else {
                        (0, 0)
                    };
                    let ingest_ms = (kr.ingest() as i64 - 62_167_219_200) * 1000;
                    if kr.ingest() == M40 || (ingest_ms as i128) - (t as i128) > LATE as i128 {
                        break 'v;
                    }
                    let uh = uhash(uid);
                    ctx.links.prefetch(uh);
                    let gofs = (uid.as_ptr() as usize).wrapping_add(gb) as u64;
                    let pr = P { pkey: 0, gp: gofs | (price as u64) << 40, ts: (t - ctx.base) as u32, cat, dead: 0 };
                    pend.push((uh, kr, pr));
                    if pend.len() >= BATCH {
                        flush(ctx, pool, pb, pend, k, ev);
                    }
                    return;
                }
                k.push(pool, (kr.w0 >> (64 - pb)) as usize, kr);
            });
            flush(ctx, pool, pb, pend, k, ev);
        } else {
            st.pending.clear();
            csv::scan::<4>(&m[s..e], &mut st.buf, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match ctx.campaigns.get_fast(f[2], room(f[2])) {
                    Some(c) => c,
                    None => return,
                };
                let t = match ts::parse(f[3]) {
                    Some(t) if t >= ctx.base && t < ctx.we => t,
                    _ => return,
                };
                let uh = uhash(f[1]);
                let c = C1 { pkey: 0, cptr: store.ptr(fi, basep, f[0]), ts: (t - ctx.base) as u32, camp };
                st.pending.push((uh, c));
            });
            // Resolve people with prefetching: the link table is too big for cache.
            let n = st.pending.len();
            for i in 0..n {
                if i + PREFETCH < n {
                    ctx.links.prefetch(st.pending[i + PREFETCH].0);
                }
                let (uh, mut c) = st.pending[i];
                c.pkey = ctx.person(uh);
                st.cl.push(pool, ctx.qpart(c.pkey), c);
            }
        }
        buf::fence();
    };

    // One pool for pass 1. Priority: link work (parse + union per link chunk;
    // then, once every chunk is in, naming) > clicks (they need the people) >
    // events (parsed meanwhile; their copies wait in `pend` until the links
    // are done, so no thread idles at the link phase barriers).
    let link_next = AtomicUsize::new(0);
    let link_done = AtomicUsize::new(0);
    let unioned = AtomicBool::new(link_chunks.is_empty());
    let name_next = AtomicUsize::new(0);
    let name_done = AtomicUsize::new(0);
    let n_name_tasks = cap.div_ceil(SLOT_TASK);
    let main_next = [AtomicUsize::new(0), AtomicUsize::new(n_ev_tasks)];
    let link_counts = [AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0)]; // pairs, ids, people
    let link_step = |st: &mut S1| -> bool {
        let t = if link_next.load(Relaxed) < link_chunks.len() { link_next.fetch_add(1, Relaxed) } else { usize::MAX };
        if t < link_chunks.len() {
            let (fi, s, e, _) = link_chunks[t];
            let m = &store.maps[fi];
            let basep = m.as_ptr();
            let pairs = &mut st.pairs;
            pairs.clear();
            csv::scan::<2>(&m[s..e], &mut st.buf, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                pairs.push((uhash(f[0]), store.ptr(fi, basep, f[0]), uhash(f[1]), store.ptr(fi, basep, f[1])));
            });
            let idmap = &ctx.links;
            for (k, &(ha, pa, hb, pb)) in pairs.iter().enumerate() {
                if let Some(&(h2, _, h3, _)) = pairs.get(k + PREFETCH) {
                    idmap.prefetch(h2);
                    idmap.prefetch(h3);
                }
                let (a, new_a) = idmap.insert(ha);
                if new_a {
                    idptr[a].store(pa, Relaxed);
                }
                let (b, new_b) = idmap.insert(hb);
                if new_b {
                    idptr[b].store(pb, Relaxed);
                }
                loop {
                    let ra = find(&parent, a);
                    let rb = find(&parent, b);
                    if ra == rb {
                        break;
                    }
                    let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
                    if parent[hi].compare_exchange(0, lo as u32 + 1, Relaxed, Relaxed).is_ok() {
                        break;
                    }
                }
            }
            link_counts[0].fetch_add(pairs.len(), Relaxed);
            if link_done.fetch_add(1, AcqRel) + 1 == link_chunks.len() {
                unioned.store(true, Release);
            }
            return true;
        }
        if !unioned.load(Acquire) || name_next.load(Relaxed) >= n_name_tasks {
            return false;
        }
        let t = name_next.fetch_add(1, Relaxed);
        if t >= n_name_tasks {
            return false;
        }
        // Each ID's value becomes its root; each root's name is the byte-wise smallest ID.
        let (idmap, comp_name) = (&ctx.links, &ctx.comp_name);
        let mut cnt = (0usize, 0usize);
        for i in t * SLOT_TASK..((t + 1) * SLOT_TASK).min(cap) {
            if !idmap.occupied(i) {
                continue;
            }
            cnt.0 += 1;
            let r = find(&parent, i);
            cnt.1 += (r == i) as usize;
            idmap.set_val(i, r as u32);
            let p = idptr[i].load(Relaxed);
            let name = &comp_name[r];
            let mut cur = name.load(Relaxed);
            while cur == 0 || store.get(p) < store.get(cur) {
                match name.compare_exchange(cur, p, Relaxed, Relaxed) {
                    Ok(_) => break,
                    Err(now) => cur = now,
                }
            }
        }
        link_counts[1].fetch_add(cnt.0, Relaxed);
        link_counts[2].fetch_add(cnt.1, Relaxed);
        if name_done.fetch_add(1, AcqRel) + 1 == n_name_tasks {
            let c: Vec<usize> = link_counts.iter().map(|c| c.load(Relaxed)).collect();
            lap(&format!("links: {} lines, {} ids, {} people", c[0], c[1], c[2]));
            ctx.linked.store(true, Release);
        }
        true
    };
    let s1: Vec<S1> = std::thread::scope(|sc| {
        let hs: Vec<_> = s1
            .into_iter()
            .map(|mut st| {
                let (link_step, chunk_task, main_next, ctx, pool, n_tasks) = (&link_step, &chunk_task, &main_next, &ctx, &pool, main_chunks.len());
                sc.spawn(move || {
                    let mut idle = 0u32;
                    loop {
                        if link_step(&mut st) {
                            continue;
                        }
                        let linked = ctx.linked.load(Acquire);
                        if linked {
                            if !st.pend.is_empty() {
                                let S1 { k, ev, pend, .. } = &mut st;
                                flush(ctx, pool, pb, pend, k, ev);
                                buf::fence();
                            }
                            let t = main_next[1].fetch_add(1, Relaxed);
                            if t < n_tasks {
                                chunk_task(&mut st, t);
                                continue;
                            }
                        }
                        let t = main_next[0].fetch_add(1, Relaxed);
                        if t < n_ev_tasks {
                            chunk_task(&mut st, t);
                            continue;
                        }
                        if linked {
                            break;
                        }
                        // Only clicks are left, and they wait for the links.
                        idle += 1;
                        if idle < 64 {
                            std::hint::spin_loop();
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop((parent, idptr));
    let mut s1 = s1;
    for st in &mut s1 {
        st.k.finish();
        st.ev.finish();
        st.cl.finish();
    }
    let n_k: usize = s1.iter().map(|s| s.k.total()).sum();
    let n_p: usize = s1.iter().map(|s| s.ev.total()).sum();
    let n_c1: usize = s1.iter().map(|s| s.cl.total()).sum();
    let cpu_ev: f64 = s1.iter().map(|s| s.cpu[1]).sum();
    let cpu_cl: f64 = s1.iter().map(|s| s.cpu[2]).sum();
    lap(&format!("pass 1: parse ({n_k} event copies, {n_p} valid, {n_c1} valid clicks; cpu events {cpu_ev:.2}s clicks {cpu_cl:.2}s)"));

    // ---- pass 2: dedup events. Per event_id: the copy with the smallest
    // ingest wins (equal ingest: identical copies, keep one); every other
    // copy's P record is marked dead. An invalid winner kills all its copies.
    // A 2-bit saturating counter per hash bucket first finds the keys that
    // may repeat; only their copies go through the exact table.
    par_run((0..nthreads).map(|_| (Vec::<u64>::new(), Vec::<K>::new())).collect(), np, |(filter, table), p| {
        let n: usize = s1.iter().map(|s| s.k.len(p)).sum();
        if n == 0 {
            return;
        }
        let fbits = (n * 8).next_power_of_two().max(64);
        let fmask = fbits - 1;
        filter.clear();
        filter.resize(fbits / 32, 0);
        let slot = |r: &K| {
            let i = (r.w0 as usize) & fmask;
            (i >> 5, (i & 31) * 2)
        };
        let mut suspects = 0usize;
        for s in &s1 {
            for b in s.k.blocks(p) {
                for r in b {
                    let (w, sh) = slot(r);
                    let f = unsafe { filter.get_unchecked_mut(w) };
                    let v = (*f >> sh) & 3;
                    suspects += [0, 2, 1, 1][v as usize];
                    *f += ((v < 2) as u64) << sh;
                }
            }
        }
        if suspects == 0 {
            return;
        }
        let size = (suspects * 2).next_power_of_two().max(16);
        let mask = size - 1;
        table.clear();
        table.resize(size, K::default());
        for s in &s1 {
            for b in s.k.blocks(p) {
                for r in b {
                    let (w, sh) = slot(r);
                    if (unsafe { *filter.get_unchecked(w) } >> sh) & 3 != 2 {
                        continue;
                    }
                    let mut i = ((r.w0 >> 32) as usize) & mask;
                    loop {
                        let t = unsafe { table.get_unchecked_mut(i) };
                        if t.w0 == 0 {
                            *t = *r;
                            break;
                        }
                        if t.same(r) {
                            if r.ingest() < t.ingest() {
                                t.kill();
                                *t = *r;
                            } else {
                                r.kill();
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
        }
    });
    let (s1_cl, s1_ev): (Vec<Scatter<C1>>, Vec<Scatter<P>>) = s1.into_iter().map(|s| (s.cl, s.ev)).unzip();
    lap("pass 2: dedup");

    // ---- pass 3: sessions
    // A person's events are grouped into GAP-wide bins of ts. A session never
    // breaks inside a bin, and between two consecutive non-empty bins it breaks
    // iff min(later) - max(earlier) > GAP, so ordering each person's events by
    // bin (two counting sorts per partition) replaces sorting them by ts.
    let nbins = (we - ws) as usize / GAP as usize + 1;
    let use_bins = nbins <= 1 << 16;
    struct S3 {
        users: Vec<URow>,
        camp: Vec<[u64; 4]>, // (sessions, purchasing, revenue, assisted) per (campaign, day)
        cnt: Vec<u32>,       // per campaign: its clicks in the current window
        since: Vec<u32>,     // per campaign: the session its current assist run began at
        sday: Vec<u32>,      // per session of the current person: its day index
        dfirst: Vec<u32>,    // per day index: the current person's first session on or after it
        local: Vec<u64>,     // local person table keys (u64::MAX = empty)
        local_id: Vec<u32>,  // local person table values
        pkeys: Vec<u64>,     // per local person: its key
        pname: Vec<u64>,     // per local person: its name
        pstart: Vec<u32>,    // per local person: offset into the grouped events (n+1)
        pos: Vec<u32>,
        bstart: Vec<u32>,    // per bin: offset (n+1)
        cpid: Vec<u32>,      // per click: local person
        cstart: Vec<u32>,    // per local person: offset into cls (n+1)
        r1: Vec<R>,
        r2: Vec<R>,
        cls: Vec<C1>,
        cats: Vec<(u32, u64)>,
    }
    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct R {
        ts: u32,
        pid: u32,
        price: u32,
        cat: u32,
    }
    #[inline(always)]
    fn bin(ts: u32) -> usize {
        ((ts - WEEK as u32) / GAP) as usize
    }
    let s3: Vec<S3> = (0..nthreads)
        .map(|_| S3 {
            users: Vec::new(),
            camp: vec![[0u64; 4]; (ncamp + 1) * ndays],
            cnt: vec![0; ncamp],
            since: vec![0; ncamp],
            sday: Vec::new(),
            dfirst: vec![0; ndays + 1],
            local: Vec::new(),
            local_id: Vec::new(),
            pkeys: Vec::new(),
            pname: Vec::new(),
            pstart: Vec::new(),
            pos: Vec::new(),
            bstart: Vec::new(),
            cpid: Vec::new(),
            cstart: Vec::new(),
            r1: Vec::new(),
            r2: Vec::new(),
            cls: Vec::new(),
            cats: Vec::new(),
        })
        .collect();
    fn each<T: Copy + Default>(v: &[Scatter<T>], q: usize, mut f: impl FnMut(&T)) {
        for s in v {
            for b in s.blocks(q) {
                for r in b {
                    f(r);
                }
            }
        }
    }
    /// Exclusive prefix sums in place; returns the total.
    fn prefix(v: &mut [u32]) -> u32 {
        let mut acc = 0u32;
        for c in v.iter_mut() {
            let x = *c;
            *c = acc;
            acc += x;
        }
        acc
    }
    #[inline(always)]
    fn local_find(st: &S3, pkey: u64) -> u32 {
        let mask = st.local.len() - 1;
        let mut k = (mix64(pkey) as usize) & mask;
        loop {
            let key = unsafe { *st.local.get_unchecked(k) };
            if key == pkey {
                return unsafe { *st.local_id.get_unchecked(k) };
            }
            if key == u64::MAX {
                return u32::MAX;
            }
            k = (k + 1) & mask;
        }
    }
    fn insert_slot(st: &mut S3, pkey: u64, id: u32) {
        let mask = st.local.len() - 1;
        let mut k = (mix64(pkey) as usize) & mask;
        while st.local[k] != u64::MAX {
            k = (k + 1) & mask;
        }
        st.local[k] = pkey;
        st.local_id[k] = id;
    }
    // Largest partitions first (a whale person's can be many times the mean),
    // so no big one is left for the end.
    let mut q_order: Vec<(usize, usize)> =
        (0..nq).map(|q| (s1_ev.iter().map(|s| s.len(q)).sum::<usize>() + s1_cl.iter().map(|s| s.len(q)).sum::<usize>(), q)).collect();
    q_order.sort_unstable_by(|a, b| b.cmp(a));
    // Each worker writes its user_daily file as soon as it runs out of partitions.
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let write_users = |t: usize, st: &mut S3| {
        if !st.users.is_empty() || t == 0 {
            out::write_user_daily(&ud.join(format!("part-{t:03}.parquet")), &st.users, &store, &ctx.categories)
                .expect("write user_daily");
        }
    };
    let s3 = par_run_fin(s3, nq, |st, t| {
        let q = q_order[t].1;
        let n: usize = s1_ev.iter().map(|s| s.len(q)).sum();
        if n == 0 {
            return;
        }
        // Local person ids, in a table that grows with the people seen.
        let size = (n / 8).next_power_of_two().max(1024);
        st.local.clear();
        st.local.resize(size, u64::MAX);
        st.local_id.resize(size, 0);
        st.pkeys.clear();
        st.pname.clear();
        st.pstart.clear();
        st.r1.clear();
        if use_bins {
            st.bstart.clear();
            st.bstart.resize(nbins + 1, 0);
        }
        each(&s1_ev, q, |e| {
            if e.dead != 0 {
                return;
            }
            let mut id = local_find(st, e.pkey);
            if id == u32::MAX {
                id = st.pkeys.len() as u32;
                st.pkeys.push(e.pkey);
                st.pname.push(if e.pkey & LINKED != 0 {
                    ctx.comp_name[(e.pkey & !LINKED) as usize].load(Relaxed)
                } else {
                    Store::field_name(e.gp & M40)
                });
                st.pstart.push(0);
                if st.pkeys.len() * 2 > st.local.len() {
                    let size = st.local.len() * 2;
                    st.local.clear();
                    st.local.resize(size, u64::MAX);
                    st.local_id.resize(size, 0);
                    for i in 0..st.pkeys.len() {
                        insert_slot(st, st.pkeys[i], i as u32);
                    }
                } else {
                    insert_slot(st, e.pkey, id);
                }
            }
            unsafe { *st.pstart.get_unchecked_mut(id as usize) += 1 };
            if use_bins {
                unsafe { *st.bstart.get_unchecked_mut(bin(e.ts)) += 1 };
            }
            st.r1.push(R { ts: e.ts, pid: id, price: e.price(), cat: e.cat as u32 });
        });
        let npeople = st.pkeys.len();
        let m = st.r1.len();
        let mut r1 = std::mem::take(&mut st.r1);
        let mut r2 = std::mem::take(&mut st.r2);
        r2.clear();
        r2.resize(m, R::default());
        if use_bins {
            // By bin, then stably by person.
            prefix(&mut st.bstart);
            for r in &r1 {
                let b = unsafe { st.bstart.get_unchecked_mut(bin(r.ts)) };
                unsafe { *r2.get_unchecked_mut(*b as usize) = *r };
                *b += 1;
            }
            std::mem::swap(&mut r1, &mut r2);
        }
        let total = prefix(&mut st.pstart);
        st.pstart.push(total);
        st.pos.clear();
        st.pos.extend_from_slice(&st.pstart[..npeople]);
        for r in &r1 {
            let p = unsafe { st.pos.get_unchecked_mut(r.pid as usize) };
            unsafe { *r2.get_unchecked_mut(*p as usize) = *r };
            *p += 1;
        }
        let evs = r2;
        st.r2 = r1;
        // Clicks of these people, grouped the same way.
        st.cstart.clear();
        st.cstart.resize(npeople + 1, 0);
        st.cpid.clear();
        each(&s1_cl, q, |c| {
            let id = local_find(st, c.pkey);
            st.cpid.push(id);
            if id != u32::MAX {
                st.cstart[id as usize] += 1;
            }
        });
        let total = prefix(&mut st.cstart);
        let mut cls = std::mem::take(&mut st.cls);
        cls.clear();
        cls.resize(total as usize, C1::default());
        st.pos.clear();
        st.pos.extend_from_slice(&st.cstart[..npeople]);
        let mut i = 0;
        each(&s1_cl, q, |c| {
            let id = st.cpid[i];
            i += 1;
            if id != u32::MAX {
                cls[st.pos[id as usize] as usize] = *c;
                st.pos[id as usize] += 1;
            }
        });
        for p in 0..npeople {
            let (a, b) = (st.pstart[p] as usize, st.pstart[p + 1] as usize);
            let (ca, cb) = (st.cstart[p] as usize, st.cstart[p + 1] as usize);
            let pc = &mut cls[ca..cb];
            if pc.len() > 1 {
                pc.sort_unstable_by_key(|c| c.ts);
            }
            let pe = &evs[a..b];
            if !use_bins {
                let mut v = pe.to_vec();
                v.sort_unstable_by_key(|e| e.ts);
                person(st, &ctx, &store, st.pname[p], &v, &cls[ca..cb], ncamp);
            } else {
                person(st, &ctx, &store, st.pname[p], pe, &cls[ca..cb], ncamp);
            }
        }
        st.r1 = evs;
        st.cls = cls;
    }, write_users);

    /// Sessions `a..b` of the current person (whose latest session is on day
    /// `cur`) were assisted by campaign `c`.
    #[inline]
    fn assist(st: &mut S3, ndays: usize, cur: u32, c: usize, a: u32, b: u32) {
        if a >= b {
            return;
        }
        let (d0, d1) = (st.sday[a as usize], st.sday[b as usize - 1]);
        if d0 == d1 {
            st.camp[c * ndays + d0 as usize][3] += (b - a) as u64;
            return;
        }
        for d in d0..=d1 {
            let lo = a.max(st.dfirst[d as usize]);
            let hi = if d < cur { b.min(st.dfirst[d as usize + 1]) } else { b };
            if hi > lo {
                st.camp[c * ndays + d as usize][3] += (hi - lo) as u64;
            }
        }
    }

    /// One person: `evs` ordered by bin, `cls` sorted by ts.
    fn person(st: &mut S3, ctx: &Ctx, store: &Store, name: u64, evs: &[R], cls: &[C1], ncamp: usize) {
        let ndays = ctx.ndays;
        let n = evs.len();
        st.sday.clear();
        st.cats.clear();
        let (mut lo, mut hi) = (0usize, 0usize);
        // The current user-day.
        let mut cur_day = u32::MAX;
        let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
        let flush = |st: &mut S3, day: u32, s: u32, e: u32, r: u64| {
            let mut top = u32::MAX;
            let mut best = 0u64;
            for &(c, v) in &st.cats {
                if v > best || (v == best && c < top) {
                    best = v;
                    top = c;
                }
            }
            st.users.push(URow { name, day: (ctx.day0 + day as i64) as i32, sessions: s, events: e, revenue: r, top });
            st.cats.clear();
        };
        // Ends a session: credit and assists.
        let mut end = |st: &mut S3, start: u32, di: u32, revenue: u64, purchase: bool| {
            let g = st.sday.len() as u32;
            st.sday.push(di);
            if cls.is_empty() {
                let a = &mut st.camp[ncamp * ndays + di as usize];
                a[0] += 1;
                a[1] += purchase as u64;
                a[2] += revenue;
                return;
            }
            // Window: clicks with start - 7d <= ts <= start.
            while hi < cls.len() && cls[hi].ts <= start {
                let c = cls[hi].camp as usize;
                if st.cnt[c] == 0 {
                    st.since[c] = g;
                }
                st.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < start as i64 - WEEK {
                let c = cls[lo].camp as usize;
                st.cnt[c] -= 1;
                if st.cnt[c] == 0 {
                    let s = st.since[c];
                    assist(st, ndays, di, c, s, g);
                }
                lo += 1;
            }
            let credited = if lo < hi {
                let t = cls[hi - 1].ts;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cls[k - 1].ts == t {
                    k -= 1;
                    if store.get(cls[k].cptr) < store.get(cls[best].cptr) {
                        best = k;
                    }
                }
                cls[best].camp as usize
            } else {
                ncamp
            };
            let a = &mut st.camp[credited * ndays + di as usize];
            a[0] += 1;
            a[1] += purchase as u64;
            a[2] += revenue;
        };
        let mut i = 0usize;
        let mut prev_mx = 0u32;
        let (mut s_start, mut s_day, mut s_rev, mut s_purchase) = (0u32, 0u32, 0u64, false);
        while i < n {
            // One bin.
            let b = bin(evs[i].ts);
            let (mut mn, mut mx) = (evs[i].ts, evs[i].ts);
            let mut j = i + 1;
            while j < n && bin(evs[j].ts) == b {
                mn = mn.min(evs[j].ts);
                mx = mx.max(evs[j].ts);
                j += 1;
            }
            if i == 0 || mn - prev_mx > GAP {
                if i != 0 {
                    end(st, s_start, s_day, s_rev, s_purchase);
                }
                s_start = mn;
                s_day = ((ctx.base + mn as i64).div_euclid(DAY) - ctx.day0) as u32;
                s_rev = 0;
                s_purchase = false;
                if s_day != cur_day {
                    if cur_day != u32::MAX {
                        flush(st, cur_day, d_sessions, d_events, d_revenue);
                    }
                    let from = if cur_day == u32::MAX { s_day } else { cur_day + 1 };
                    let g = st.sday.len() as u32;
                    for d in from..=s_day {
                        st.dfirst[d as usize] = g;
                    }
                    cur_day = s_day;
                    d_sessions = 0;
                    d_events = 0;
                    d_revenue = 0;
                }
                d_sessions += 1;
            }
            d_events += (j - i) as u32;
            for e in &evs[i..j] {
                if e.price > 0 {
                    s_purchase = true;
                    s_rev += e.price as u64;
                    d_revenue += e.price as u64;
                    match st.cats.iter_mut().find(|(c, _)| *c == e.cat) {
                        Some(x) => x.1 += e.price as u64,
                        None => st.cats.push((e.cat, e.price as u64)),
                    }
                }
            }
            prev_mx = mx;
            i = j;
        }
        end(st, s_start, s_day, s_rev, s_purchase);
        flush(st, cur_day, d_sessions, d_events, d_revenue);
        // Close the assist runs still open.
        let g = st.sday.len() as u32;
        for k in lo..hi {
            let c = cls[k].camp as usize;
            if st.cnt[c] != 0 {
                st.cnt[c] = 0;
                let s = st.since[c];
                assist(st, ndays, cur_day, c, s, g);
            }
        }
    }
    std::mem::forget((s1_ev, s1_cl, pool));
    lap("pass 3: sessions");

    // ---- output (user_daily was written at the end of pass 3)
    let mut camp_total = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in &s3 {
        for (a, b) in camp_total.iter_mut().zip(&s.camp) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    out::write_campaign_daily(&cd.join("part-000.parquet"), &camp_total, &ctx, ncamp).expect("write campaign_daily");
    lap("output");
    // Exit without unmapping or freeing, after dropping the big mappings in parallel.
    let inputs: Vec<(usize, usize)> = store.maps.iter().map(|m| (m.as_ptr() as usize, m.len())).collect();
    buf::release_and_exit(nthreads, &inputs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_order_and_keys() {
        let a = ingest_secs(b"2026-10-03T23:58:10Z");
        assert_eq!(a, ingest_secs(b"1791071890000"));
        assert!(ingest_secs(b"2026-10-03T23:58:11Z") == a + 1);
        assert!(ingest_secs(b"0000-01-01T00:00:00Z") < ingest_secs(b"1969-12-31T23:59:59Z"));
        assert!(ingest_secs(b"9999-12-31T23:59:59Z") < M40);
        assert_eq!(ingest_secs(b"nope"), M40);
        assert_eq!((a as i64 - 62_167_219_200) * 1000, 1_791_071_890_000);
        let h = table::ekey(b"e1");
        let k1 = K::new(h, 5);
        let mut k2 = K::new(h, 9);
        k2.w2 |= 0x7fff_1234_5678;
        assert!(k1.same(&k2) && k1.ingest() == 5 && k2.ingest() == 9);
        assert!(!k1.same(&K::new(h ^ (1 << 100), 5)));
        assert!(!k1.same(&K::new(h ^ (1 << 70), 5)));
        assert!(!k1.same(&K::new(h ^ 1, 5)));
        let mut p = P::default();
        let kp = K { w2: (k1.w2 & !M48) | (&mut p as *mut P as u64), ..k1 };
        kp.kill();
        assert_eq!(p.dead, 1);
        K::new(h, 1).kill(); // null: no-op
    }
}
