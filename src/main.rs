//! Shopping Sessions pipeline. See rules.md.
//!
//! Phases:
//!   0. products, campaigns, manifest; links -> union-find -> person names.
//!   1. parse event and click chunks in parallel. Event copies are scattered by
//!      event_id hash; valid clicks are resolved to a person and scattered by person.
//!   2. per event_id partition: dedup (smallest ingest_ts), keep surviving kept
//!      copies, resolve person, scatter by person.
//!   3. per person partition: sort, sessionize, attribute, aggregate.
//!   4. write Parquet.

mod csv;
mod out;
mod ts;

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
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
    #[inline]
    fn write_u128(&mut self, v: u128) {
        self.0 = v as u64;
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

/// All input files, memory-mapped. A `Ptr` names a byte range in one of them:
/// file (12 bits) | offset (36 bits) | len (16 bits). File 4095 means the
/// overflow arena, with the offset as an index.
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
        if len < 0xFFFF && off < (1usize << 36) && (file as u64) < OVERFLOW_FILE {
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

// ---------------------------------------------------------------- lookups

/// Exact byte-string -> index map (hash plus verification).
struct Dict {
    map: HashMap<u64, u32, IdBuild>,
    keys: Vec<Vec<u8>>,
}

impl Dict {
    fn new() -> Self {
        Dict { map: HashMap::default(), keys: Vec::new() }
    }
    /// Insert if absent; returns the index.
    fn insert(&mut self, k: &[u8]) -> u32 {
        let h = xxh3_64(k);
        if let Some(&i) = self.map.get(&h) {
            if self.keys[i as usize] == k {
                return i;
            }
            panic!("dictionary hash collision");
        }
        let i = self.keys.len() as u32;
        self.keys.push(k.to_vec());
        self.map.insert(h, i);
        i
    }
    #[inline]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = xxh3_64(k);
        match self.map.get(&h) {
            Some(&i) if self.keys[i as usize] == k => Some(i),
            _ => None,
        }
    }
}

pub struct Ctx {
    ws: i64,
    we: i64,
    /// Base for the u32 relative timestamps: window_start - 7 days.
    base: i64,
    products: Dict,
    /// product index -> category rank
    product_cat: Vec<u32>,
    unknown_cat: u32,
    /// category names sorted byte-wise; index = rank
    pub categories: Vec<Vec<u8>>,
    campaigns: Dict,
    pub channels: Vec<Vec<u8>>, // per campaign index
    /// user_id hash -> component, for every ID named in a valid link line
    links: HashMap<u64, u32, IdBuild>,
    /// component -> Ptr of its name
    comp_name: Vec<u64>,
    pub day0: i64,
    pub ndays: usize,
}

impl Ctx {
    #[inline]
    fn person(&self, uhash: u64) -> u64 {
        match self.links.get(&uhash) {
            Some(&c) => LINKED | c as u64,
            None => uhash & !LINKED,
        }
    }
    #[inline]
    fn category(&self, product: &[u8]) -> u32 {
        match self.products.get(product) {
            Some(i) => self.product_cat[i as usize],
            None => self.unknown_cat,
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E1 {
    eid_lo: u64,
    eid_hi: u64,
    ingest: i64,
    uhash: u64,
    uptr: u64,
    ts: u32,
    price: u32,
    cat: u32,
    ok: u32,
}

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

fn map_file(p: &Path) -> Mmap {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        // memmap2 can't map an empty file; map an anonymous empty region instead.
        return memmap2::MmapOptions::new().len(1).map_anon().unwrap().make_read_only().unwrap();
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    m
}

/// Data byte ranges of a file (after its header line), split near `CHUNK` at newlines.
fn chunks_of(data: &[u8], real_len: usize) -> Vec<(usize, usize)> {
    let data = &data[..real_len];
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

fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
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
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
    let lap = |what: &str| eprintln!("[{:>8.3}s] {what}", t0.elapsed().as_secs_f64());

    // ---- manifest, products, campaigns
    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let base = ws - WEEK;
    assert!(we - base < u32::MAX as i64, "window too long");

    let mut products = Dict::new();
    let mut product_cat_name: Vec<u32> = Vec::new();
    let mut cat_dict = Dict::new();
    read_small::<2>(&data_dir.join("products.csv"), |f| {
        let before = products.keys.len();
        let i = products.insert(f[0]);
        let c = cat_dict.insert(f[1]);
        if i as usize == before {
            product_cat_name.push(c);
        }
    });
    // Categories ranked byte-wise, with "unknown" added (it never appears in the file).
    let unknown_raw = cat_dict.insert(b"unknown");
    let mut order: Vec<u32> = (0..cat_dict.keys.len() as u32).collect();
    order.sort_by(|&a, &b| cat_dict.keys[a as usize].cmp(&cat_dict.keys[b as usize]));
    let mut rank = vec![0u32; order.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u32;
    }
    let categories: Vec<Vec<u8>> = order.iter().map(|&c| cat_dict.keys[c as usize].clone()).collect();
    let product_cat: Vec<u32> = product_cat_name.iter().map(|&c| rank[c as usize]).collect();
    let unknown_cat = rank[unknown_raw as usize];

    let mut campaigns = Dict::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    read_small::<2>(&data_dir.join("campaigns.csv"), |f| {
        let before = campaigns.keys.len();
        let i = campaigns.insert(f[0]);
        if i as usize == before {
            channels.push(f[1].to_vec());
        }
    });
    let ncamp = campaigns.keys.len();

    let day0 = floor_div(ws, DAY);
    let ndays = (floor_div(we - 1, DAY) - day0 + 1) as usize;

    // ---- map every CSV
    let mut paths: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, sub) in [(0u8, "links"), (1, "events"), (2, "clicks")] {
        for p in list_csv(&data_dir.join(sub)) {
            paths.push((kind, p));
        }
    }
    assert!(paths.len() < OVERFLOW_FILE as usize, "too many files");
    let mut real_len = Vec::new();
    let maps: Vec<Mmap> = paths
        .iter()
        .map(|(_, p)| {
            let m = map_file(p);
            real_len.push(std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0));
            m
        })
        .collect();
    let store = Store { maps, overflow: Mutex::new(Vec::new()) };
    let mut link_chunks = Vec::new();
    let mut main_chunks = Vec::new();
    for (fi, (kind, _)) in paths.iter().enumerate() {
        for (s, e) in chunks_of(&store.maps[fi], real_len[fi]) {
            if *kind == 0 {
                link_chunks.push((fi, s, e, 0u8));
            } else {
                main_chunks.push((fi, s, e, *kind));
            }
        }
    }
    // Big chunks first is unnecessary (chunks are ~equal); interleave kinds.
    lap("setup");

    // ---- links
    type LinkPair = (u64, u64, u64, u64); // (ha, pa, hb, pb)
    let link_states: Vec<Vec<LinkPair>> = (0..nthreads).map(|_| Vec::new()).collect();
    let link_states = par_run(link_states, link_chunks.len(), |st, t| {
        let (fi, s, e, _) = link_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        for_lines(&m[s..e], |line| {
            let mut f: [&[u8]; 2] = [&[]; 2];
            if !csv::split::<2>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                return;
            }
            st.push((xxh3_64(f[0]), store.ptr(fi, basep, f[0]), xxh3_64(f[1]), store.ptr(fi, basep, f[1])));
        });
    });
    let nlinks: usize = link_states.iter().map(|v| v.len()).sum();
    let mut idmap: HashMap<u64, u32, IdBuild> = HashMap::with_capacity_and_hasher(nlinks * 2, IdBuild::default());
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
                *idmap.entry(h).or_insert_with(|| {
                    idptr.push(p);
                    parent.push(parent.len() as u32);
                    (parent.len() - 1) as u32
                })
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
    drop(parent);
    drop(comp_of);
    drop(comp_of_root);
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
        channels,
        links: idmap,
        comp_name,
        day0,
        ndays,
    };

    // ---- pass 1: events and clicks
    struct S1 {
        ev: Vec<Vec<E1>>,
        cl: Vec<Vec<C1>>,
    }
    let s1: Vec<S1> = (0..nthreads)
        .map(|_| S1 { ev: (0..NP).map(|_| Vec::new()).collect(), cl: (0..NQ).map(|_| Vec::new()).collect() })
        .collect();
    let s1 = par_run(s1, main_chunks.len(), |st, t| {
        let (fi, s, e, kind) = main_chunks[t];
        let m = &store.maps[fi];
        let basep = m.as_ptr();
        let ctx = &ctx;
        if kind == 1 {
            for_lines(&m[s..e], |line| {
                let mut f: [&[u8]; 7] = [&[]; 7];
                if !csv::split::<7>(line, &mut f) {
                    return;
                }
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let h = xxh3_128(eid);
                let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
                let mut r = E1 { eid_lo: h as u64, eid_hi: (h >> 64) as u64, ingest, ..Default::default() };
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
                    r.uhash = xxh3_64(uid);
                    r.uptr = store.ptr(fi, basep, uid);
                }
                st.ev[(r.eid_hi >> (64 - PB)) as usize].push(r);
            });
        } else {
            for_lines(&m[s..e], |line| {
                let mut f: [&[u8]; 4] = [&[]; 4];
                if !csv::split::<4>(line, &mut f) {
                    return;
                }
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
                let pkey = ctx.person(xxh3_64(f[1]));
                st.cl[qpart(pkey)].push(C1 { pkey, cptr: store.ptr(fi, basep, f[0]), ts: (t - ctx.base) as u32, camp });
            });
        }
    });
    lap("pass 1: parse");

    // ---- pass 2: dedup events
    struct S2 {
        out: Vec<Vec<E2>>,
        map: HashMap<u128, u64, IdBuild>,
    }
    let s2: Vec<S2> = (0..nthreads)
        .map(|_| S2 { out: (0..NQ).map(|_| Vec::new()).collect(), map: HashMap::default() })
        .collect();
    let s2 = par_run(s2, NP, |st, p| {
        st.map.clear();
        for (ti, s) in s1.iter().enumerate() {
            for (i, r) in s.ev[p].iter().enumerate() {
                let key = ((r.eid_hi as u128) << 64) | r.eid_lo as u128;
                let loc = ((ti as u64) << 40) | i as u64;
                match st.map.entry(key) {
                    hashbrown::hash_map::Entry::Vacant(v) => {
                        v.insert(loc);
                    }
                    hashbrown::hash_map::Entry::Occupied(mut o) => {
                        let cur = *o.get();
                        let c = &s1[(cur >> 40) as usize].ev[p][(cur & ((1 << 40) - 1)) as usize];
                        if r.ingest < c.ingest {
                            o.insert(loc);
                        }
                    }
                }
            }
        }
        for &loc in st.map.values() {
            let r = &s1[(loc >> 40) as usize].ev[p][(loc & ((1 << 40) - 1)) as usize];
            if r.ok == 0 {
                continue;
            }
            let pkey = ctx.person(r.uhash);
            st.out[qpart(pkey)].push(E2 { pkey, uptr: r.uptr, ts: r.ts, price: r.price, cat: r.cat, _pad: 0 });
        }
    });
    let s1_cl: Vec<Vec<Vec<C1>>> = s1.into_iter().map(|s| s.cl).collect();
    lap("pass 2: dedup");

    // ---- pass 3: sessions
    struct S3 {
        users: Vec<URow>,
        camp: Vec<[u64; 4]>, // (sessions, purchasing, revenue, assisted) per (campaign, day)
        stamp: Vec<u32>,
        serial: u32,
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
            evs: Vec::new(),
            cls: Vec::new(),
            cats: Vec::new(),
        })
        .collect();
    let s3 = par_run(s3, NQ, |st, q| {
        st.evs.clear();
        for s in &s2 {
            st.evs.extend_from_slice(&s.out[q]);
        }
        st.cls.clear();
        for s in &s1_cl {
            st.cls.extend_from_slice(&s[q]);
        }
        st.evs.sort_unstable_by_key(|e| (e.pkey, e.ts));
        st.cls.sort_unstable_by_key(|c| (c.pkey, c.ts));
        let evs = std::mem::take(&mut st.evs);
        let cls = std::mem::take(&mut st.cls);
        let mut ci = 0usize;
        let mut i = 0usize;
        while i < evs.len() {
            let pkey = evs[i].pkey;
            let mut j = i + 1;
            while j < evs.len() && evs[j].pkey == pkey {
                j += 1;
            }
            while ci < cls.len() && cls[ci].pkey < pkey {
                ci += 1;
            }
            let mut cj = ci;
            while cj < cls.len() && cls[cj].pkey == pkey {
                cj += 1;
            }
            person(st, &ctx, &store, &evs[i..j], &cls[ci..cj], ncamp);
            ci = cj;
            i = j;
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
            let day = floor_div(ctx.base + start as i64, DAY);
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
    drop(s2);
    drop(s1_cl);
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
}
