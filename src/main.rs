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
mod table;
mod ts;

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use buf::Parts;
use memmap2::Mmap;
use table::{prefetch, uhash, IdIndex, U64Map};
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

pub const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;

const PB: u32 = 9; // event_id partitions = 2^PB
const QB: u32 = 9; // person partitions = 2^QB
const NP: usize = 1 << PB;
const NQ: usize = 1 << QB;
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

#[inline]
fn qpart(pkey: u64) -> usize {
    (mix64(pkey) >> (64 - QB)) as usize
}

// ---------------------------------------------------------------- byte store

/// All input files, memory-mapped. A `Ptr` (u64) names a byte range in one of
/// them: file (12 bits) | offset (36 bits) | len (16 bits). File 4095 means
/// the overflow arena, with the offset as an index.
pub struct Store {
    maps: Vec<Mmap>,
    overflow: Mutex<Vec<&'static [u8]>>,
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
    /// user_id hash -> component, for every ID named in a valid link line
    links: U64Map,
    /// component -> Ptr of its name
    comp_name: Vec<u64>,
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
    fn category(&self, product: &[u8]) -> u16 {
        match self.products.get(product) {
            Some(i) => self.product_cat[i as usize],
            None => self.unknown_cat,
        }
    }
}

// ---------------------------------------------------------------- records

/// One parsed event copy (pass 1).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E1 {
    eid: u64,
    ingest: i64,
    uhash: u64,
    uptr: u64,
    eid2: u32,
    ts: u32,
    price: u32,
    cat: u16,
    ok: u16,
}

/// A surviving event (pass 2).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E2 {
    pkey: u64,
    uptr: u64,
    ts: u32,
    price: u32,
    cat: u32,
    _pad: u32,
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

fn minflt() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let after = &s[s.rfind(')')? + 2..];
            after.split(' ').nth(7)?.parse().ok()
        })
        .unwrap_or(0)
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
    let lap = |what: &str| eprintln!("[{:>7.3}s] {what}  (minflt {})", t0.elapsed().as_secs_f64(), minflt());

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
    let store = Store { maps, overflow: Mutex::new(Vec::new()) };
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
    let link_states: Vec<Vec<LinkPair>> = (0..nthreads).map(|_| Vec::new()).collect();
    let link_states = par_run(link_states, link_chunks.len(), |st, t| {
        let (fi, s, e, _) = link_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        csv::scan::<2>(&m[s..e], |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            st.push((uhash(f[0]), store.ptr(fi, basep, f[0]), uhash(f[1]), store.ptr(fi, basep, f[1])));
        });
    });
    let nlinks: usize = link_states.iter().map(|v| v.len()).sum();
    let mut idmap = U64Map::with_capacity(nlinks * 2 + 16);
    let mut idptr: Vec<u64> = Vec::with_capacity(nlinks * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(nlinks * 2);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let gp = p[p[x as usize] as usize];
            p[x as usize] = gp;
            x = gp;
        }
        x
    }
    for v in &link_states {
        for &(ha, pa, hb, pb) in v {
            let mut id = |h: u64, p: u64| -> u32 {
                let (i, new) = idmap.get_or_insert(h, parent.len() as u32);
                if new {
                    idptr.push(p);
                    parent.push(i);
                }
                i
            };
            let a = id(ha, pa);
            let b = id(hb, pb);
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    drop(link_states);
    // Components and their names (byte-wise smallest ID).
    let nid = parent.len();
    let mut comp_of_root = vec![u32::MAX; nid];
    let mut comp_name: Vec<u64> = Vec::new();
    let mut comp_of = vec![0u32; nid];
    for i in 0..nid {
        let r = find(&mut parent, i as u32) as usize;
        if comp_of_root[r] == u32::MAX {
            comp_of_root[r] = comp_name.len() as u32;
            comp_name.push(idptr[i]);
        } else {
            let c = comp_of_root[r] as usize;
            if store.get(idptr[i]) < store.get(comp_name[c]) {
                comp_name[c] = idptr[i];
            }
        }
        comp_of[i] = comp_of_root[r];
    }
    for v in idmap.values_mut() {
        *v = comp_of[*v as usize];
    }
    drop((parent, comp_of, comp_of_root, idptr));
    lap(&format!("links: {nlinks} lines, {nid} ids, {} people", comp_name.len()));

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
        day0,
        ndays,
    };

    // ---- pass 1: events and clicks
    let est_events = bytes[1] / 70 + 1024;
    let est_clicks = bytes[2] / 45 + 1024;
    let ev_cap = est_events / (nthreads * NP) * 13 / 10;
    let cl_cap = est_clicks / (nthreads * NQ) * 13 / 10;
    struct S1 {
        ev: Parts<E1>,
        cl: Parts<C1>,
        pending: Vec<(u64, C1)>,
    }
    let s1: Vec<S1> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| sc.spawn(|| S1 { ev: Parts::new(NP, ev_cap), cl: Parts::new(NQ, cl_cap), pending: Vec::new() }))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let s1 = par_run(s1, main_chunks.len(), |st, t| {
        let (fi, s, e, kind) = main_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        let ctx = &ctx;
        if kind == 1 {
            csv::scan::<7>(&m[s..e], |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let h = xxh3_128(eid);
                let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
                let mut r = E1 { eid: h as u64, eid2: (h >> 64) as u32, ingest, ..Default::default() };
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
                        match csv::price(f[6]) {
                            Some(p) => p,
                            None => break 'v,
                        }
                    } else {
                        0
                    };
                    if (ingest as i128) - (t as i128) > LATE as i128 {
                        break 'v;
                    }
                    r.ok = 1;
                    r.ts = (t - ctx.base) as u32;
                    r.price = price;
                    r.cat = ctx.category(f[5]);
                    r.uhash = uhash(uid);
                    r.uptr = store.ptr(fi, basep, uid);
                }
                st.ev.push((r.eid >> (64 - PB)) as usize, r);
            });
        } else {
            st.pending.clear();
            csv::scan::<4>(&m[s..e], |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match ctx.campaigns.get(f[2]) {
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
                st.cl.push(qpart(c.pkey), c);
            }
        }
    });
    let n_e1: usize = s1.iter().map(|s| s.ev.total()).sum();
    let n_c1: usize = s1.iter().map(|s| s.cl.total()).sum();
    lap(&format!("pass 1: parse ({n_e1} event copies, {n_c1} valid clicks)"));

    // ---- pass 2: dedup events
    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct DSlot {
        eid: u64,
        eid2: u32,
        loc: u32, // thread (5 bits) | index (27 bits); u32::MAX = empty
    }
    let e2_cap = n_e1 / (nthreads * NQ) * 3 / 2 + 16;
    struct S2 {
        out: Parts<E2>,
        table: Vec<DSlot>,
        winners: Vec<u32>,
    }
    let s2: Vec<S2> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| sc.spawn(|| S2 { out: Parts::new(NQ, e2_cap), table: Vec::new(), winners: Vec::new() }))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let s2 = par_run(s2, NP, |st, p| {
        let n: usize = s1.iter().map(|s| s.ev.len(p)).sum();
        let size = (n * 2).next_power_of_two().max(16);
        let mask = size - 1;
        st.table.clear();
        st.table.resize(size, DSlot { eid: 0, eid2: 0, loc: u32::MAX });
        let rec = |loc: u32| -> &E1 { s1[(loc >> 27) as usize].ev.get(p, (loc & ((1 << 27) - 1)) as usize) };
        for (ti, s) in s1.iter().enumerate() {
            for (i, r) in s.ev.iter(p).enumerate() {
                let loc = ((ti as u32) << 27) | i as u32;
                let mut k = (r.eid as usize) & mask;
                loop {
                    let slot = &mut st.table[k];
                    if slot.loc == u32::MAX {
                        *slot = DSlot { eid: r.eid, eid2: r.eid2, loc };
                        break;
                    }
                    if slot.eid == r.eid && slot.eid2 == r.eid2 {
                        if r.ingest < rec(slot.loc).ingest {
                            slot.loc = loc;
                        }
                        break;
                    }
                    k = (k + 1) & mask;
                }
            }
        }
        st.winners.clear();
        for slot in &st.table {
            if slot.loc != u32::MAX && rec(slot.loc).ok != 0 {
                st.winners.push(slot.loc);
            }
        }
        let w = std::mem::take(&mut st.winners);
        for i in 0..w.len() {
            if i + PREFETCH < w.len() {
                ctx.links.prefetch(rec(w[i + PREFETCH]).uhash);
            }
            let r = rec(w[i]);
            let pkey = ctx.person(r.uhash);
            st.out.push(qpart(pkey), E2 { pkey, uptr: r.uptr, ts: r.ts, price: r.price, cat: r.cat as u32, _pad: 0 });
        }
        st.winners = w;
    });
    let (s1_cl, s1_ev): (Vec<Parts<C1>>, Vec<Parts<E1>>) = s1.into_iter().map(|s| (s.cl, s.ev)).unzip();
    // Never freed: the process exits without unmapping, which is faster.
    std::mem::forget(s1_ev);
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
        evs: Vec<E2>,
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
    let s3 = par_run(s3, NQ, |st, q| {
        let n: usize = s2.iter().map(|s| s.out.len(q)).sum();
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
        for s in &s2 {
            for e in s.out.iter(q) {
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
        }
        // Counting sort events by person.
        let mut acc = 0u32;
        for c in st.start.iter_mut() {
            let v = *c;
            *c = acc;
            acc += v;
        }
        st.start.push(acc);
        st.evs.clear();
        st.evs.resize(n, E2::default());
        {
            let mut pos: Vec<u32> = st.start[..npeople as usize].to_vec();
            let mut i = 0;
            for s in &s2 {
                for e in s.out.iter(q) {
                    let id = st.pid[i] as usize;
                    st.evs[pos[id] as usize] = *e;
                    pos[id] += 1;
                    i += 1;
                }
            }
        }
        // Clicks of these people, grouped the same way.
        st.cstart.clear();
        st.cstart.resize(npeople as usize + 1, 0);
        st.pid.clear();
        for s in &s1_cl {
            for c in s.iter(q) {
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
        }
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
            for s in &s1_cl {
                for c in s.iter(q) {
                    let id = st.pid[i];
                    i += 1;
                    if id != u32::MAX {
                        st.cls[pos[id as usize] as usize] = *c;
                        pos[id as usize] += 1;
                    }
                }
            }
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

    fn person(st: &mut S3, ctx: &Ctx, store: &Store, evs: &[E2], cls: &[C1], ncamp: usize) {
        let name = if evs[0].pkey & LINKED != 0 {
            ctx.comp_name[(evs[0].pkey & !LINKED) as usize]
        } else {
            evs[0].uptr
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
                if e.price > 0 {
                    purchase = true;
                    revenue += e.price as u64;
                    match st.cats.iter_mut().find(|(c, _)| *c == e.cat) {
                        Some(x) => x.1 += e.price as u64,
                        None => st.cats.push((e.cat, e.price as u64)),
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
    std::mem::forget((s2, s1_cl));
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
