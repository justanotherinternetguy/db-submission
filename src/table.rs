//! Small open-addressing tables tuned for this pipeline.

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// An 8-byte load that may extend outside the Rust object `p` points into
/// (the caller guarantees the bytes are mapped).
#[inline(always)]
unsafe fn load8_raw(p: *const u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let v: u64;
        std::arch::asm!("mov {v}, qword ptr [{p}]", p = in(reg) p, v = out(reg) v,
            options(pure, readonly, nostack, preserves_flags));
        v
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        (p as *const u64).read_unaligned()
    }
}

#[inline(always)]
fn mum(a: u64, b: u64) -> (u64, u64) {
    let p = (a as u128) * (b as u128);
    (p as u64, (p >> 64) as u64)
}

// wyhash-style secrets; every byte has its high bit set, so no ASCII word
// XORs one to zero (which would zero the product).
const S0: u64 = 0xa076_1d64_78bd_e42f | 0x8080_8080_8080_8080;
const S1: u64 = 0xe703_7ed1_a0b4_28db | 0x8080_8080_8080_8080;
const S2: u64 = 0x8ebc_6af0_9c88_c6e3 | 0x8080_8080_8080_8080;
const S3: u64 = 0x5899_65cc_7537_4cc3 | 0x8080_8080_8080_8080;

/// wyhash's 4..16-byte finish on the words (a, b) and length.
#[inline(always)]
fn wy(a: u64, b: u64, len: u64, s0: u64, s1: u64) -> u64 {
    let (lo, hi) = mum(a ^ s1, b ^ s0);
    let (lo, hi) = mum(lo ^ s0 ^ len, hi ^ s1);
    lo ^ hi
}

/// For a slice of at most 16 bytes, (a, b): a = its first min(8, len) bytes
/// zero-padded, b = its last 8 bytes if len > 8, else 0. With the length they
/// determine the slice. Branch-free (8 vs 9 bytes is a coin flip) except for
/// the rare slice whose 8-byte neighbourhood crosses a page.
#[inline(always)]
fn words16(s: &[u8]) -> (u64, u64) {
    let len = s.len();
    debug_assert!(len <= 16);
    let p = s.as_ptr() as usize;
    let end = p + len;
    if len >= 8 || ((p & 4095) <= 4088 && len > 0) {
        let a = unsafe { load8_raw(p as *const u8) };
        let a = if len >= 8 { a } else { a & ((1u64 << (8 * len)) - 1) };
        let b = if len > 8 { unsafe { load8_raw((end - 8) as *const u8) } } else { 0 };
        (a, b)
    } else {
        let mut t = [0u8; 8];
        t[..len].copy_from_slice(s);
        (u64::from_le_bytes(t), 0)
    }
}

/// A user_id hash, never 0 (0 marks an empty slot). 64 bits: ~4M distinct
/// user IDs at 60M events give a collision chance of about n^2 / 2^65 ~ 1e-6.
#[inline(always)]
pub fn uhash(b: &[u8]) -> u64 {
    let len = b.len();
    let h = if len <= 16 {
        let (x, y) = words16(b);
        wy(x, y, len as u64, S0, S1)
    } else {
        xxh3_64(b)
    };
    if h == 0 {
        1
    } else {
        h
    }
}

/// The event_id dedup key, 128 bits (the pipeline keeps 104). Up to 8 bytes
/// the key is a bijection of (bytes, length): exact. 9 to 16 bytes: two
/// independent wyhash finishes; over 16 bytes: xxh3-128. With 60M distinct
/// IDs a random 104-bit key collides with chance n^2 / 2^105 ~ 1e-16; keys of
/// the exact class differ from random ones in the high word except by chance.
#[inline(always)]
pub fn ekey(b: &[u8]) -> u128 {
    let len = b.len();
    if len <= 16 {
        let (x, y) = words16(b);
        // splitmix64's finalizer is a bijection; the high word marks the length.
        let short = ((0xFFFF_FFFF_FFFF_FF00 | len as u128) << 64) | mix64(x) as u128;
        let long = ((wy(x, y, len as u64, S2, S3) as u128) << 64) | wy(x, y, len as u64, S0, S1) as u128;
        if len <= 8 {
            short
        } else {
            long
        }
    } else {
        xxh3_128(b)
    }
}

#[inline(always)]
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Exact lookup of short IDs like `p123` or `c_45`: when every key is `prefix`
/// followed by a canonical decimal number, a flat array indexed by the number;
/// otherwise a verified hash map.
pub struct IdIndex {
    prefix: Vec<u8>,
    /// `prefix` as a little-endian word (when shorter than 8 bytes)
    pword: u64,
    flat: Option<Vec<u32>>,
    map: hashbrown::HashMap<u64, u32, crate::IdBuild>,
    keys: Vec<Vec<u8>>,
}

const MAX_FLAT: u64 = 1 << 24;

#[inline]
fn canonical_number(k: &[u8], prefix: &[u8]) -> Option<u64> {
    let rest = k.strip_prefix(prefix)?;
    if rest.is_empty() || rest.len() > 8 || (rest[0] == b'0' && rest.len() > 1) {
        return None;
    }
    let mut v = 0u64;
    for &c in rest {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
    }
    Some(v)
}

impl IdIndex {
    /// Maps each of the distinct `keys` to its position.
    pub fn build(keys: &[Vec<u8>], prefix: &[u8]) -> Self {
        let mut flat = None;
        let nums: Option<Vec<u64>> = keys.iter().map(|k| canonical_number(k, prefix)).collect();
        if let Some(nums) = nums {
            let max = nums.iter().copied().max().unwrap_or(0);
            if max < MAX_FLAT {
                let mut t = vec![u32::MAX; max as usize + 1];
                for (i, n) in nums.iter().enumerate() {
                    t[*n as usize] = i as u32;
                }
                flat = Some(t);
            }
        }
        let mut map = hashbrown::HashMap::default();
        if flat.is_none() {
            for (i, k) in keys.iter().enumerate() {
                map.insert(xxh3_64(k), i as u32);
            }
        }
        let mut pw = [0u8; 8];
        let pl = prefix.len().min(8);
        pw[..pl].copy_from_slice(&prefix[..pl]);
        IdIndex { prefix: prefix.to_vec(), pword: u64::from_le_bytes(pw), flat, map, keys: keys.to_vec() }
    }

    /// `get`, reading up to 8 bytes before the key's end when `room`.
    #[inline(always)]
    pub fn get_fast(&self, k: &[u8], room: bool) -> Option<u32> {
        if let Some(t) = &self.flat {
            let len = k.len();
            let pl = self.prefix.len();
            if room && len <= 8 && len > pl && pl < 8 {
                // The key is the top `len` bytes of the 8 ending at its end.
                let w = unsafe { (k.as_ptr().add(len).sub(8) as *const u64).read_unaligned() };
                let key = w >> (8 * (8 - len));
                let pmask = (1u64 << (8 * pl)) - 1;
                let pword = self.pword;
                let dl = len - pl;
                let low = 8 * (8 - dl); // bits before the digits, replaced by '0'
                let mask = (1u64 << low) - 1;
                let d = (w & !mask) | (0x3030_3030_3030_3030 & mask);
                let first = (w >> low) & 0xFF;
                let ok = ((key & pmask) == pword) & crate::ts::digits8(d) & ((first != b'0' as u64) | (dl == 1));
                if !ok {
                    return None;
                }
                return match t.get(crate::ts::value8(d) as usize) {
                    Some(&v) if v != u32::MAX => Some(v),
                    _ => None,
                };
            }
        }
        self.get(k)
    }

    #[inline]
    pub fn get(&self, k: &[u8]) -> Option<u32> {
        if let Some(t) = &self.flat {
            let n = canonical_number(k, &self.prefix)?;
            return match t.get(n as usize) {
                Some(&v) if v != u32::MAX => Some(v),
                _ => None,
            };
        }
        let &i = self.map.get(&xxh3_64(k))?;
        if self.keys[i as usize].as_slice() == k {
            Some(i)
        } else {
            None
        }
    }
}

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

#[repr(C)]
struct ASlot {
    key: AtomicU64,
    val: AtomicU32,
    _pad: u32,
}

/// Concurrent insert-only u64 -> slot-index table (non-zero keys), then a
/// read-only u64 -> u32 map once values are set. Backed by huge pages.
pub struct AtomicMap {
    _region: crate::buf::Region,
    slots: *const ASlot,
    shift: u32,
    mask: usize,
}
unsafe impl Send for AtomicMap {}
unsafe impl Sync for AtomicMap {}

impl AtomicMap {
    /// `max_keys` must bound the number of distinct keys.
    pub fn new(max_keys: usize) -> Self {
        let bits = ((max_keys.max(8) * 2).next_power_of_two().trailing_zeros()).max(4);
        let n = 1usize << bits;
        let region = crate::buf::Region::new(n * std::mem::size_of::<ASlot>());
        let slots = region.ptr() as *const ASlot; // zeroed by mmap
        AtomicMap { _region: region, slots, shift: 64 - bits, mask: n - 1 }
    }

    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline(always)]
    fn slot(&self, i: usize) -> &ASlot {
        unsafe { &*self.slots.add(i) }
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        prefetch(unsafe { self.slots.add((h >> self.shift) as usize) });
    }

    /// Slot index of `h`, inserting it if absent; true if this call inserted it.
    #[inline]
    pub fn insert(&self, h: u64) -> (usize, bool) {
        let mut i = (h >> self.shift) as usize;
        loop {
            let s = self.slot(i);
            let k = s.key.load(Relaxed);
            if k == h {
                return (i, false);
            }
            if k == 0 {
                match s.key.compare_exchange(0, h, Relaxed, Relaxed) {
                    Ok(_) => return (i, true),
                    Err(now) if now == h => return (i, false),
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h >> self.shift) as usize;
        loop {
            let s = self.slot(i);
            let k = s.key.load(Relaxed);
            if k == h {
                return Some(s.val.load(Relaxed));
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    pub fn occupied(&self, i: usize) -> bool {
        self.slot(i).key.load(Relaxed) != 0
    }

    #[inline]
    pub fn set_val(&self, i: usize, v: u32) {
        self.slot(i).val.store(v, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_at_page_edges() {
        let mut v = vec![0u8; 3 * 4096];
        for (i, x) in v.iter_mut().enumerate() {
            *x = (i * 7 + 3) as u8;
        }
        let base = v.as_ptr() as usize;
        let page = (base / 4096 + 1) * 4096 - base; // offset of a page start
        for end in page - 20..page + 20 {
            for len in 0..=16 {
                let b = &v[end - len..end];
                let mut t = [0u8; 16];
                t[..len.min(8)].copy_from_slice(&b[..len.min(8)]);
                if len > 8 {
                    t[8..].copy_from_slice(&b[len - 8..]);
                }
                let want = (u64::from_le_bytes(t[..8].try_into().unwrap()), u64::from_le_bytes(t[8..].try_into().unwrap()));
                assert_eq!(words16(b), want);
                let b = &v[end..end + len];
                let c = b.to_vec();
                assert_eq!(words16(b), words16(&c));
                let c = b.to_vec();
                assert_eq!(uhash(b), uhash(&c));
                assert_eq!(ekey(b), ekey(&c));
            }
        }
    }

    #[test]
    fn keys_distinct() {
        let mut seen = std::collections::HashMap::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzU\0 \"";
        for _ in 0..1_000_000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let len = (state >> 59) as usize % 18 + (state >> 40) as usize % 2 * 16;
            let mut id = Vec::new();
            for k in 0..len {
                let r = state.rotate_left(k as u32 * 5).wrapping_mul(0x2545_F491_4F6C_DD1D);
                id.push(alphabet[(r >> 58) as usize % alphabet.len()]);
            }
            let k = ekey(&id);
            if let Some(prev) = seen.insert(k, id.clone()) {
                assert_eq!(prev, id, "ekey collision");
            }
        }
        // Exactness up to 8 bytes: every 1- and 2-byte string, NULs included.
        let mut s2 = std::collections::HashSet::new();
        for a in 0..=255u8 {
            assert!(s2.insert(ekey(&[a])));
            for b in 0..=255u8 {
                assert!(s2.insert(ekey(&[a, b])));
            }
        }
        assert!(s2.insert(ekey(b"")));
    }

    #[test]
    fn get_fast_matches_get() {
        for (prefix, n) in [(&b"p"[..], 50_000u32), (&b"c_"[..], 2000)] {
            let mut keys: Vec<Vec<u8>> = (0..n).map(|i| [prefix, (i * 3 + 1).to_string().as_bytes()].concat()).collect();
            keys.swap(0, 7);
            let idx = IdIndex::build(&keys, prefix);
            let oracle: std::collections::HashMap<Vec<u8>, u32> = keys.iter().enumerate().map(|(i, k)| (k.clone(), i as u32)).collect();
            assert!(idx.flat.is_some());
            let alphabet = b"pc_0123456789P\"x\r ";
            let mut state = 0x1234_5678_9abc_def1u64;
            let mut next = || {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as usize
            };
            for round in 0..400_000 {
                let mut c: Vec<u8> = if round % 2 == 0 {
                    keys[next() % keys.len()].clone()
                } else {
                    (0..next() % 11).map(|_| alphabet[next() % alphabet.len()]).collect()
                };
                if round % 3 == 0 && !c.is_empty() {
                    let i = next() % c.len();
                    c[i] = alphabet[next() % alphabet.len()];
                }
                let mut buf = b"0123456789,9,\"x,".to_vec();
                buf.extend_from_slice(&c);
                buf.extend_from_slice(b",junk");
                let f = &buf[16..16 + c.len()];
                assert_eq!(idx.get_fast(f, true), idx.get(f), "{:?}", String::from_utf8_lossy(&c));
                assert_eq!(idx.get(f), oracle.get(&c).copied());
            }
        }
    }
}
