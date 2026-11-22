//! Pass-1 parse micro-benchmark (not used by the pipeline).
//!
//!   bench DATA_DIR [MB] [REPS]
//!
//! Loads up to MB megabytes of event files (and clicks) into memory and reports
//! the best ns/line over REPS runs for: scan only, scan+ts, scan+hash, full
//! event record build, and full click build.

#![allow(dead_code)]

#[path = "../buf.rs"]
mod buf;
#[path = "../csv.rs"]
mod csv;
#[path = "../table.rs"]
mod table;
#[path = "../ts.rs"]
mod ts;

use std::hash::{BuildHasherDefault, Hasher};
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Default, Clone, Copy)]
pub struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0 = xxh3_64(bytes) ^ self.0.rotate_left(29);
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
pub type IdBuild = BuildHasherDefault<IdHasher>;


fn load(dir: &Path, mb: usize) -> Vec<u8> {
    let mut paths: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    // A fake header line so every field has 8 readable bytes before its end.
    let mut v = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n".to_vec();
    for p in paths {
        let b = std::fs::read(&p).unwrap();
        let s = memchr::memchr(b'\n', &b).map_or(b.len(), |i| i + 1);
        v.extend_from_slice(&b[s..]);
        if v.len() > mb << 20 {
            break;
        }
    }
    v
}

fn read_small(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let b = std::fs::read(path).unwrap();
    let mut out = Vec::new();
    for line in b.split(|&c| c == b'\n').skip(1) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if csv::split::<2>(line, &mut f) {
            out.push((f[0].to_vec(), f[1].to_vec()));
        }
    }
    out
}

/// A user-space hardware counter of this thread (perf_event_open).
struct Counter(i32);
impl Counter {
    fn new(config: u64) -> Option<Counter> {
        let mut attr = [0u64; 17];
        attr[0] = (136u64 << 32) | 0; // size, type = PERF_TYPE_HARDWARE
        attr[1] = config;
        attr[5] = (1 << 0) | (1 << 5) | (1 << 6); // disabled, exclude_kernel, exclude_hv
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) } as i32;
        if fd < 0 {
            None
        } else {
            Some(Counter(fd))
        }
    }
    fn start(&self) {
        unsafe {
            libc::ioctl(self.0, 0x2403, 0);
            libc::ioctl(self.0, 0x2400, 0);
        }
    }
    fn stop(&self) -> u64 {
        let mut v = 0u64;
        unsafe {
            libc::ioctl(self.0, 0x2401, 0);
            libc::read(self.0, &mut v as *mut u64 as *mut libc::c_void, 8);
        }
        v
    }
}

fn bench(name: &str, lines: usize, reps: usize, mut f: impl FnMut() -> u64) {
    let cnt: Vec<Option<Counter>> = [0u64, 1, 5].iter().map(|&c| Counter::new(c)).collect();
    let mut best = f64::MAX;
    let mut best_c = [u64::MAX; 3];
    let mut sink = 0;
    for _ in 0..reps {
        cnt.iter().flatten().for_each(|c| c.start());
        let t = Instant::now();
        sink ^= f();
        best = best.min(t.elapsed().as_secs_f64());
        for (i, c) in cnt.iter().enumerate() {
            if let Some(c) = c {
                best_c[i] = best_c[i].min(c.stop());
            }
        }
    }
    black_box(sink);
    let l = lines as f64;
    eprintln!(
        "{name:<14} {:7.2} ns/line  {:6.1} cyc  {:6.1} ins  IPC {:.2}  {:.3} brmiss/line",
        best * 1e9 / l,
        best_c[0] as f64 / l,
        best_c[1] as f64 / l,
        best_c[1] as f64 / best_c[0] as f64,
        best_c[2] as f64 / l
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let mb: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);
    let reps: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);
    let only: Option<&str> = args.get(4).map(|s| s.as_str());
    let run = |n: &str| only.map_or(true, |o| o.split(',').any(|x| x == n));
    black_box(f1_probe(black_box(b"2026-10-03T23:58:10Z")));
    let data = load(&dir.join("events"), mb);
    let manifest = std::fs::read_to_string(dir.join("manifest.json")).unwrap();
    let grab = |k: &str| {
        let at = manifest.find(k).unwrap();
        let r = &manifest[at + k.len() + 1..];
        let q1 = r.find('"').unwrap();
        let r = &r[q1 + 1..];
        let q2 = r.find('"').unwrap();
        ts::parse(r[..q2].as_bytes()).unwrap()
    };
    let ws = grab("window_start\"");
    let we = grab("window_end\"");
    let base = ws - 7 * 86_400_000;
    let prods = read_small(&dir.join("products.csv"));
    let pkeys: Vec<Vec<u8>> = prods.iter().map(|p| p.0.clone()).collect();
    let products = table::IdIndex::build(&pkeys, b"p");
    let pcat: Vec<u16> = (0..pkeys.len()).map(|i| (i % 20) as u16).collect();

    let hdr = memchr::memchr(b'\n', &data).unwrap() + 1;
    let body = &data[hdr..];
    let mut buf = Vec::new();
    let mut lines = 0usize;
    csv::scan::<7>(body, &mut buf, |_| lines += 1);
    if let Some(l) = std::env::var("LINES").ok().and_then(|s| s.parse().ok()) { lines = l; }
    eprintln!("{} MB, {} lines, {:.1} bytes/line", body.len() >> 20, lines, body.len() as f64 / lines as f64);
    let basep = data.as_ptr();
    let room = |f: &[u8]| f.as_ptr() as usize + f.len() >= basep as usize + 8;

    // Sub-chunks like the pipeline (4 MiB).
    let mut chunks = Vec::new();
    let mut s = 0;
    while s < body.len() {
        let mut e = (s + (4 << 20)).min(body.len());
        if e < body.len() {
            e = memchr::memchr(b'\n', &body[e..]).map_or(body.len(), |i| e + i + 1);
        }
        chunks.push((s, e));
        s = e;
    }

    if run("scan") {
        bench("scan", lines, reps, || {
            let mut x = 0u64;
            for &(s, e) in &chunks {
                x = x.wrapping_add(scan7_probe(&body[s..e], &mut buf));
            }
            x
        });
    }
    if run("ts") {
        bench("scan+ts", lines, reps, || {
            let mut x = 0u64;
            for &(s, e) in &chunks {
                csv::scan::<7>(&body[s..e], &mut buf, |f| {
                    x = x.wrapping_add(ts::parse(f[2]).unwrap_or(7) as u64 ^ ts::parse(f[3]).unwrap_or(3) as u64)
                });
            }
            x
        });
    }
    if run("hash") {
        bench("scan+hash", lines, reps, || {
            let mut x = 0u64;
            for &(s, e) in &chunks {
                csv::scan::<7>(&body[s..e], &mut buf, |f| {
                    x = x.wrapping_add(table::ekey(f[0]) as u64 ^ table::uhash(f[1]))
                });
            }
            x
        });
    }
    if run("cat") {
        bench("scan+cat", lines, reps, || {
            let mut x = 0u64;
            for &(s, e) in &chunks {
                csv::scan::<7>(&body[s..e], &mut buf, |f| {
                    let c = match products.get_fast(f[5], room(f[5])) {
                        Some(i) => pcat[i as usize],
                        None => 99,
                    };
                    x = x.wrapping_add(c as u64 ^ csv::price_fast(f[6], room(f[6])).unwrap_or(1) as u64)
                });
            }
            x
        });
    }
    macro_rules! one {
        ($name:expr, $f:ident, $e:expr) => {
            if run($name) {
                bench($name, lines, reps, || {
                    let mut x = 0u64;
                    for &(s, e) in &chunks {
                        csv::scan::<7>(&body[s..e], &mut buf, |$f| x = x.wrapping_add($e));
                    }
                    x
                });
            }
        };
    }
    one!("ts2", f, ts::parse(f[2]).unwrap_or(7) as u64);
    one!("ts3", f, ts::parse(f[3]).unwrap_or(7) as u64);
    one!("h128", f, table::ekey(f[0]) as u64);
    one!("h64", f, table::uhash(f[1]));
    one!("cat1", f, match products.get_fast(f[5], room(f[5])) { Some(i) => pcat[i as usize] as u64, None => 99 });
    one!("price", f, csv::price_fast(f[6], room(f[6])).unwrap_or(1) as u64);
    if run("full") {
        // Mirrors the pass-1 event closure of main.rs (24 B key + 24 B person
        // records), minus the person lookup and scatter.
        const M40: u64 = (1 << 40) - 1;
        let ingest_secs = |s: &[u8]| -> u64 {
            match ts::parse_f1(s).or_else(|| ts::parse(s)) {
                Some(ms) => (ms.div_euclid(1000) + 62_167_219_200).clamp(0, M40 as i64 - 1) as u64,
                None => M40,
            }
        };
        let mut keys: Vec<[u64; 3]> = Vec::with_capacity(lines + 16);
        let mut pend: Vec<(u64, [u64; 3], [u64; 3])> = Vec::with_capacity(lines + 16);
        bench("full", lines, reps, || {
            keys.clear();
            pend.clear();
            for &(s, e) in &chunks {
                csv::scan::<7>(&body[s..e], &mut buf, |f| {
                    let eid = f[0];
                    if eid.is_empty() {
                        return;
                    }
                    let h = table::ekey(eid);
                    let ing = ingest_secs(f[3]);
                    let kr = [(h as u64).max(1), (((h >> 64) as u64) << 40) | ing, ((h >> 88) as u64) << 48];
                    'v: {
                        let uid = f[1];
                        if uid.is_empty() {
                            break 'v;
                        }
                        let t = match ts::parse(f[2]) {
                            Some(t) if t >= ws && t < we => t,
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
                        let ingest_ms = (ing as i64 - 62_167_219_200) * 1000;
                        if ing == M40 || (ingest_ms as i128) - (t as i128) > 86_400_000 {
                            break 'v;
                        }
                        let uh = table::uhash(uid);
                        let cat = match products.get_fast(f[5], room(f[5])) {
                            Some(i) => pcat[i as usize],
                            None => 99,
                        };
                        let gofs = (uid.as_ptr() as usize - basep as usize) as u64;
                        pend.push((uh, kr, [0, gofs | (price as u64) << 40, ((t - base) as u64) | (cat as u64) << 32]));
                        return;
                    }
                    keys.push(kr);
                });
            }
            (keys.len() + pend.len()) as u64
        });
    }
}

#[inline(never)]
#[no_mangle]
pub fn scan7_probe(data: &[u8], buf: &mut Vec<u64>) -> u64 {
    let mut x = 0u64;
    csv::scan::<7>(data, buf, |f| x = x.wrapping_add(f[0].len() as u64 ^ f[6].len() as u64));
    x
}

#[inline(never)]
#[no_mangle]
pub fn f1_probe(s: &[u8]) -> i64 {
    ts::parse_f1(s).unwrap_or(7)
}
