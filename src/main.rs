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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use table::{uhash, AtomicMap, IdIndex};
use buf::ZeroVec;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

pub const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;

const CHUNK: usize = 4 << 20;
const PREFETCH: usize = 16;

const LINKED: u64 = 1 << 63;

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
    products: IdIndex,
    /// product position -> category rank
    product_cat: Vec<u16>,
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
            Some(i) => self.product_cat[i as usize],
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
    match ts::parse(s) {
        Some(ms) => (ms.div_euclid(1000) + 62_167_219_200).clamp(0, M40 as i64 - 1) as u64,
        None => M40,
    }
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
fn dedup_keys(keys: &mut Vec<Vec<u8>>, seen: &mut hashbrown::HashMap<Vec<u8>, u32>, k: &[u8]) -> (u32, bool) {
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
        let mut seen_p = hashbrown::HashMap::new();
        let mut seen_c = hashbrown::HashMap::new();
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
    let products = IdIndex::build(&product_keys, b"p");

    let mut campaign_ids = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    {
        let mut seen = hashbrown::HashMap::new();
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
    lap("setup");

    // ---- links
    type LinkPair = (u64, u64, u64, u64); // (ha, pa, hb, pb)
    let link_states: Vec<(Vec<LinkPair>, Vec<u64>)> = (0..nthreads).map(|_| (Vec::new(), Vec::new())).collect();
    let link_states = par_run(link_states, link_chunks.len(), |(pairs, buf), t| {
        let (fi, s, e, _) = link_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        csv::scan::<2>(&m[s..e], buf, |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            pairs.push((uhash(f[0]), store.ptr(fi, basep, f[0]), uhash(f[1]), store.ptr(fi, basep, f[1])));
        });
    });
    let link_states: Vec<Vec<LinkPair>> = link_states.into_iter().map(|(p, _)| p).collect();
    let nlinks: usize = link_states.iter().map(|v| v.len()).sum();
    // Concurrent union-find over slot indices of a concurrent ID table.
    // parent[x] == 0 means x is a root; otherwise parent[x] - 1 is its parent.
    let idmap = AtomicMap::new(nlinks * 2 + 16);
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
    const LINK_TASK: usize = 1 << 16;
    let link_tasks: Vec<(usize, usize)> = link_states
        .iter()
        .enumerate()
        .flat_map(|(v, pairs)| (0..pairs.len()).step_by(LINK_TASK).map(move |s| (v, s)))
        .collect();
    par_run((0..nthreads).map(|_| ()).collect(), link_tasks.len(), |_, t| {
        let (v, s) = link_tasks[t];
        let pairs = &link_states[v][s..(s + LINK_TASK).min(link_states[v].len())];
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
    });
    drop(link_states);
    // Each ID's value becomes its root; each root's name is the byte-wise smallest ID.
    const SLOT_TASK: usize = 1 << 16;
    let counts = par_run(vec![(0usize, 0usize); nthreads], cap.div_ceil(SLOT_TASK), |cnt, t| {
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
    });
    let nid: usize = counts.iter().map(|c| c.0).sum();
    let npeople: usize = counts.iter().map(|c| c.1).sum();
    drop((parent, idptr));
    lap(&format!("links: {nlinks} lines, {nid} ids, {npeople} people"));

    // Partition counts: about 8k event copies per event_id partition.
    let est_events = bytes[1] / 70 + 1024;
    let est_clicks = bytes[2] / 45 + 1024;
    let pb: u32 = (usize::BITS - 1 - (est_events / 8000).max(16).leading_zeros()).min(14);
    let qb: u32 = pb - 1;
    let (np, nq) = (1usize << pb, 1usize << qb);
    let ctx = Ctx {
        ws,
        we,
        base,
        products,
        product_cat,
        unknown_cat,
        categories,
        campaigns,
        campaign_ids,
        channels,
        links: idmap,
        comp_name,
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
            cpu: [0.0; 3],
        })
        .collect();
    /// Resolves the people of a batch of valid copies (their link slots were
    /// prefetched when parsed) and scatters them.
    #[inline(always)]
    fn flush(ctx: &Ctx, pool: &Pool, pb: u32, pend: &mut Vec<(u64, K, P)>, k: &mut Scatter<K>, ev: &mut Scatter<P>) {
        for &(uh, mut kr, mut pr) in pend.iter() {
            pr.pkey = ctx.person(uh);
            let at = ev.push(pool, ctx.qpart(pr.pkey), pr);
            kr.w2 |= at as u64;
            k.push(pool, (kr.w0 >> (64 - pb)) as usize, kr);
        }
        pend.clear();
    }
    let s1 = par_run(s1, main_chunks.len(), |st, t| {
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
                let kr = K::new(xxh3_128(eid), ingest_secs(f[3]));
                'v: {
                    let uid = f[1];
                    if uid.is_empty() {
                        break 'v;
                    }
                    let t = match ts::parse(f[2]) {
                        Some(t) if t >= ctx.ws && t < ctx.we => t,
                        _ => break 'v,
                    };
                    let purchase = match f[4] {
                        b"view" | b"cart" => false,
                        b"purchase" => true,
                        _ => break 'v,
                    };
                    if f[5].is_empty() {
                        break 'v;
                    }
                    let price = if purchase {
                        match csv::price_fast(f[6], room(f[6])) {
                            Some(p) => p,
                            None => break 'v,
                        }
                    } else {
                        0
                    };
                    let ingest_ms = (kr.ingest() as i64 - 62_167_219_200) * 1000;
                    if kr.ingest() == M40 || (ingest_ms as i128) - (t as i128) > LATE as i128 {
                        break 'v;
                    }
                    let uh = uhash(uid);
                    ctx.links.prefetch(uh);
                    let gofs = (uid.as_ptr() as usize).wrapping_add(gb) as u64;
                    let pr = P { pkey: 0, gp: gofs | (price as u64) << 40, ts: (t - ctx.base) as u32, cat: ctx.category(f[5], room(f[5])), dead: 0 };
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
    });
    let n_k: usize = s1.iter().map(|s| s.k.total()).sum();
    let n_p: usize = s1.iter().map(|s| s.ev.total()).sum();
    let n_c1: usize = s1.iter().map(|s| s.cl.total()).sum();
    let cpu_ev: f64 = s1.iter().map(|s| s.cpu[1]).sum();
    let cpu_cl: f64 = s1.iter().map(|s| s.cpu[2]).sum();
    lap(&format!("pass 1: parse ({n_k} event copies, {n_p} valid, {n_c1} valid clicks; cpu events {cpu_ev:.2}s clicks {cpu_cl:.2}s)"));

    // ---- pass 2: dedup events. Per event_id: the copy with the smallest
    // ingest wins (equal ingest: identical copies, keep one); every other
    // copy's P record is marked dead. An invalid winner kills all its copies.
    par_run((0..nthreads).map(|_| Vec::<K>::new()).collect(), np, |table, p| {
        let n: usize = s1.iter().map(|s| s.k.len(p)).sum();
        let size = (n + n / 2).next_power_of_two().max(16);
        let mask = size - 1;
        table.clear();
        table.resize(size, K::default());
        for s in &s1 {
            for b in s.k.blocks(p) {
                for r in b {
                    let mut i = (r.w0 as usize) & mask;
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
    struct S3 {
        users: Vec<URow>,
        camp: Vec<[u64; 4]>, // (sessions, purchasing, revenue, assisted) per (campaign, day)
        stamp: Vec<u32>,
        serial: u32,
        local: Vec<u64>,      // local person table keys (u64::MAX = empty)
        local_id: Vec<u32>,   // local person table values
        pid: Vec<u32>,        // per event: local person id
        start: Vec<u32>,      // per person: offset into evs (n+1)
        cstart: Vec<u32>,     // per person: offset into cls (n+1)
        evs: Vec<P>,
        cls: Vec<C1>,
        cats: Vec<(u32, u64)>,
    }
    let s3: Vec<S3> = (0..nthreads)
        .map(|_| S3 {
            users: Vec::new(),
            camp: vec![[0u64; 4]; (ncamp + 1) * ndays],
            stamp: vec![0u32; ncamp],
            serial: 0,
            local: Vec::new(),
            local_id: Vec::new(),
            pid: Vec::new(),
            start: Vec::new(),
            cstart: Vec::new(),
            evs: Vec::new(),
            cls: Vec::new(),
            cats: Vec::new(),
        })
        .collect();
    let s3 = par_run(s3, nq, |st, q| {
        let n: usize = s1_ev.iter().map(|s| s.len(q)).sum();
        if n == 0 {
            return;
        }
        // Local person ids.
        let size = (n * 2).next_power_of_two().max(16);
        let mask = size - 1;
        st.local.clear();
        st.local.resize(size, u64::MAX);
        st.local_id.resize(size, 0);
        st.pid.clear();
        st.start.clear();
        let mut npeople = 0u32;
        each(&s1_ev, q, |e| {
            if e.dead != 0 {
                return;
            }
            {
                let mut k = (mix64(e.pkey) as usize) & mask;
                let id = loop {
                    let key = st.local[k];
                    if key == e.pkey {
                        break st.local_id[k];
                    }
                    if key == u64::MAX {
                        st.local[k] = e.pkey;
                        st.local_id[k] = npeople;
                        npeople += 1;
                        st.start.push(0);
                        break npeople - 1;
                    }
                    k = (k + 1) & mask;
                };
                st.pid.push(id);
                st.start[id as usize] += 1;
            }
        });
        // Counting sort events by person.
        let mut acc = 0u32;
        for c in st.start.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        st.start.push(acc);
        st.evs.clear();
        st.evs.resize(acc as usize, P::default());
        {
            let mut pos: Vec<u32> = st.start[..npeople as usize].to_vec();
            let mut i = 0;
            each(&s1_ev, q, |e| {
                if e.dead == 0 {
                    let id = st.pid[i] as usize;
                    st.evs[pos[id] as usize] = *e;
                    pos[id] += 1;
                    i += 1;
                }
            });
        }
        // Clicks of these people, grouped the same way.
        st.cstart.clear();
        st.cstart.resize(npeople as usize + 1, 0);
        st.pid.clear();
        each(&s1_cl, q, |c| {
            {
                let mut k = (mix64(c.pkey) as usize) & mask;
                let id = loop {
                    let key = st.local[k];
                    if key == c.pkey {
                        break st.local_id[k];
                    }
                    if key == u64::MAX {
                        break u32::MAX;
                    }
                    k = (k + 1) & mask;
                };
                st.pid.push(id);
                if id != u32::MAX {
                    st.cstart[id as usize] += 1;
                }
            }
        });
        let mut acc = 0u32;
        for c in st.cstart.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        st.cls.clear();
        st.cls.resize(acc as usize, C1::default());
        {
            let mut pos: Vec<u32> = st.cstart[..npeople as usize].to_vec();
            let mut i = 0;
            each(&s1_cl, q, |c| {
                let id = st.pid[i];
                i += 1;
                if id != u32::MAX {
                    st.cls[pos[id as usize] as usize] = *c;
                    pos[id as usize] += 1;
                }
            });
        }
        let evs = std::mem::take(&mut st.evs);
        let cls = std::mem::take(&mut st.cls);
        let mut evs = evs;
        let mut cls = cls;
        for pidx in 0..npeople as usize {
            let (a, b) = (st.start[pidx] as usize, st.start[pidx + 1] as usize);
            let (ca, cb) = (st.cstart[pidx] as usize, st.cstart[pidx + 1] as usize);
            let pe = &mut evs[a..b];
            if pe.len() > 1 {
                pe.sort_unstable_by_key(|e| e.ts);
            }
            let pc = &mut cls[ca..cb];
            if pc.len() > 1 {
                pc.sort_unstable_by_key(|c| c.ts);
            }
            person(st, &ctx, &store, &evs[a..b], &cls[ca..cb], ncamp);
        }
        st.evs = evs;
        st.cls = cls;
    });

    fn each<T: Copy>(v: &[Scatter<T>], q: usize, mut f: impl FnMut(&T)) {
        for s in v {
            for b in s.blocks(q) {
                for r in b {
                    f(r);
                }
            }
        }
    }

    fn person(st: &mut S3, ctx: &Ctx, store: &Store, evs: &[P], cls: &[C1], ncamp: usize) {
        let name = if evs[0].pkey & LINKED != 0 {
            ctx.comp_name[(evs[0].pkey & !LINKED) as usize].load(Relaxed)
        } else {
            Store::field_name(evs[0].gp & M40)
        };
        let ndays = ctx.ndays;
        let mut lo = 0usize;
        let mut hi = 0usize;
        let mut i = 0usize;
        // current user-day accumulation
        let mut cur_day: i64 = i64::MIN;
        let mut d_sessions = 0u32;
        let mut d_events = 0u32;
        let mut d_revenue = 0u64;
        st.cats.clear();
        let flush = |st: &mut S3, day: i64, s: u32, e: u32, r: u64| {
            let mut top = u32::MAX;
            let mut best = 0u64;
            for &(c, v) in &st.cats {
                if v > best || (v == best && c < top) {
                    best = v;
                    top = c;
                }
            }
            st.users.push(URow { name, day: day as i32, sessions: s, events: e, revenue: r, top });
            st.cats.clear();
        };
        while i < evs.len() {
            let start = evs[i].ts;
            let mut prev = start;
            let mut j = i;
            let mut revenue = 0u64;
            let mut purchase = false;
            let day = (ctx.base + start as i64).div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(st, cur_day, d_sessions, d_events, d_revenue);
                }
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_revenue = 0;
            }
            while j < evs.len() && evs[j].ts - prev <= GAP {
                let e = &evs[j];
                prev = e.ts;
                let price = e.price();
                if price > 0 {
                    purchase = true;
                    revenue += price as u64;
                    match st.cats.iter_mut().find(|(c, _)| *c == e.cat as u32) {
                        Some(x) => x.1 += price as u64,
                        None => st.cats.push((e.cat as u32, price as u64)),
                    }
                }
                j += 1;
            }
            d_sessions += 1;
            d_events += (j - i) as u32;
            d_revenue += revenue;
            let di = (day - ctx.day0) as usize;
            // attribution: clicks with start - 7d <= ts <= start
            while hi < cls.len() && cls[hi].ts <= start {
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < start as i64 - WEEK {
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
                st.serial = st.serial.wrapping_add(1);
                if st.serial == 0 {
                    st.stamp.iter_mut().for_each(|x| *x = 0);
                    st.serial = 1;
                }
                let serial = st.serial;
                for c in &cls[lo..hi] {
                    let ci = c.camp as usize;
                    if st.stamp[ci] != serial {
                        st.stamp[ci] = serial;
                        st.camp[ci * ndays + di][3] += 1;
                    }
                }
                cls[best].camp as usize
            } else {
                ncamp
            };
            let a = &mut st.camp[credited * ndays + di];
            a[0] += 1;
            a[1] += purchase as u64;
            a[2] += revenue;
            i = j;
        }
        flush(st, cur_day, d_sessions, d_events, d_revenue);
    }
    std::mem::forget((s1_ev, s1_cl, pool));
    lap("pass 3: sessions");

    // ---- output
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    let mut camp_total = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in &s3 {
        for (a, b) in camp_total.iter_mut().zip(&s.camp) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    std::thread::scope(|scope| {
        for (t, s) in s3.iter().enumerate() {
            let ud = &ud;
            let store = &store;
            let ctx = &ctx;
            if s.users.is_empty() && t > 0 {
                continue;
            }
            scope.spawn(move || {
                out::write_user_daily(&ud.join(format!("part-{t:03}.parquet")), &s.users, store, &ctx.categories)
                    .expect("write user_daily");
            });
        }
        let cd = &cd;
        let ctx = &ctx;
        let camp_total = &camp_total;
        scope.spawn(move || {
            out::write_campaign_daily(&cd.join("part-000.parquet"), camp_total, ctx, ncamp)
                .expect("write campaign_daily");
        });
    });
    lap("output");
    // Exit without unmapping or freeing: the kernel does it faster.
    std::process::exit(0);
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
        let h = xxh3_128(b"e1");
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
