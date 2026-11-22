//! Small open-addressing tables tuned for this pipeline.

use xxhash_rust::xxh3::xxh3_64;

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// A user_id hash, never 0 (0 marks an empty slot).
#[inline]
pub fn uhash(b: &[u8]) -> u64 {
    let h = xxh3_64(b);
    if h == 0 {
        1
    } else {
        h
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Slot {
    key: u64,
    val: u32,
    _pad: u32,
}

/// u64 (non-zero, well mixed) -> u32, linear probing, indexed by the top bits.
pub struct U64Map {
    slots: Vec<Slot>,
    shift: u32,
    mask: usize,
    len: usize,
}

impl U64Map {
    pub fn with_capacity(n: usize) -> Self {
        let bits = ((n.max(8) * 2).next_power_of_two().trailing_zeros()).max(4);
        U64Map { slots: vec![Slot::default(); 1 << bits], shift: 64 - bits, mask: (1 << bits) - 1, len: 0 }
    }

    #[inline(always)]
    fn home(&self, h: u64) -> usize {
        (h >> self.shift) as usize
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(self.home(h)) });
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = self.home(h);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.key == h {
                return Some(s.val);
            }
            if s.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns the value for `h`, inserting `val` if absent. The bool says whether it was inserted.
    #[inline]
    pub fn get_or_insert(&mut self, h: u64, val: u32) -> (u32, bool) {
        if self.len * 2 >= self.slots.len() {
            self.grow();
        }
        let mut i = self.home(h);
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.key == h {
                return (s.val, false);
            }
            if s.key == 0 {
                s.key = h;
                s.val = val;
                self.len += 1;
                return (val, true);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        *self = U64Map::with_capacity(old.len());
        for s in old {
            if s.key != 0 {
                self.get_or_insert(s.key, s.val);
            }
        }
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut u32> {
        self.slots.iter_mut().filter(|s| s.key != 0).map(|s| &mut s.val)
    }
}

/// Exact lookup of short IDs like `p123` or `c_45`: when every key is `prefix`
/// followed by a canonical decimal number, a flat array indexed by the number;
/// otherwise a verified hash map.
pub struct IdIndex {
    prefix: Vec<u8>,
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
        IdIndex { prefix: prefix.to_vec(), flat, map, keys: keys.to_vec() }
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
