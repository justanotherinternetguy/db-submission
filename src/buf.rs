//! Huge-page-advised anonymous mappings, a shared block pool, and per-thread
//! partitioned append lists built from pool blocks.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

pub struct Region {
    ptr: *mut u8,
    len: usize,
}
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

const HUGE: usize = 2 << 20;

impl Region {
    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub fn new(bytes: usize) -> Region {
        let len = bytes.max(HUGE).div_ceil(HUGE) * HUGE + HUGE;
        unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            assert!(p != libc::MAP_FAILED, "mmap {len} bytes failed");
            libc::madvise(p, len, libc::MADV_HUGEPAGE);
            // Align the usable start to a huge page.
            let aligned = (p as usize).div_ceil(HUGE) * HUGE;
            Region { ptr: aligned as *mut u8, len: len - (aligned - p as usize) }
        }
    }
}

/// A bump allocator of `BLK`-byte blocks, shared by all threads.
pub struct Pool {
    region: Region,
    next: AtomicUsize,
    extra: Mutex<(Vec<Region>, usize)>,
}
unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}

pub const BLK: usize = 4096;

impl Pool {
    /// Reserves `bytes` of address space; touched pages only are backed.
    pub fn new(bytes: usize) -> Pool {
        Pool { region: Region::new(bytes), next: AtomicUsize::new(0), extra: Mutex::new((Vec::new(), 0)) }
    }

    #[inline]
    pub fn alloc(&self) -> *mut u8 {
        let off = self.next.fetch_add(BLK, Relaxed);
        if off + BLK <= self.region.len {
            unsafe { self.region.ptr.add(off) }
        } else {
            self.alloc_slow()
        }
    }

    #[cold]
    fn alloc_slow(&self) -> *mut u8 {
        let mut g = self.extra.lock().unwrap();
        let (regions, used) = &mut *g;
        if regions.last().map_or(true, |r| *used + BLK > r.len) {
            regions.push(Region::new(256 << 20));
            *used = 0;
        }
        let p = unsafe { regions.last().unwrap().ptr.add(*used) };
        *used += BLK;
        p
    }
}

/// One thread's partitioned append lists of `T`, as chains of pool blocks.
pub struct Scatter<T: Copy> {
    /// Per part: the next write position; at `END` within a block when full.
    cur: Vec<*mut T>,
    /// Per part: its full blocks.
    full: Vec<Vec<*mut T>>,
}
unsafe impl<T: Copy + Send> Send for Scatter<T> {}
unsafe impl<T: Copy + Sync> Sync for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    /// Records per block.
    pub const PER: usize = (BLK - 1) / std::mem::size_of::<T>();
    const END: usize = Self::PER * std::mem::size_of::<T>();

    pub fn new(nparts: usize) -> Self {
        // A sentinel "full block" at address 0, so the first push allocates.
        Scatter { cur: vec![Self::END as *mut T; nparts], full: (0..nparts).map(|_| Vec::new()).collect() }
    }

    /// Appends `v` to part `p`; returns where it was written.
    #[inline(always)]
    pub fn push(&mut self, pool: &Pool, p: usize, v: T) -> *mut T {
        let mut c = unsafe { *self.cur.get_unchecked(p) };
        if c as usize & (BLK - 1) == Self::END {
            c = self.refill(pool, p, c);
        }
        unsafe {
            c.write(v);
            *self.cur.get_unchecked_mut(p) = c.add(1);
        }
        c
    }

    #[cold]
    #[inline(never)]
    fn refill(&mut self, pool: &Pool, p: usize, c: *mut T) -> *mut T {
        let start = (c as usize - Self::END) as *mut T;
        if !start.is_null() {
            self.full[p].push(start);
        }
        pool.alloc() as *mut T
    }

    /// The records of part `p`, as slices.
    pub fn blocks(&self, p: usize) -> impl Iterator<Item = &[T]> {
        let c = self.cur[p] as usize;
        let start = c & !(BLK - 1);
        let last: &[T] = if start == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(start as *const T, (c - start) / std::mem::size_of::<T>()) }
        };
        self.full[p].iter().map(|&b| unsafe { std::slice::from_raw_parts(b as *const T, Self::PER) }).chain(std::iter::once(last))
    }

    pub fn len(&self, p: usize) -> usize {
        let c = self.cur[p] as usize;
        let start = c & !(BLK - 1);
        let last = if start == 0 { 0 } else { (c - start) / std::mem::size_of::<T>() };
        self.full[p].len() * Self::PER + last
    }

    pub fn total(&self) -> usize {
        (0..self.cur.len()).map(|p| self.len(p)).sum()
    }
}

/// A zero-initialised array of a type for which all-zero bytes is a valid
/// value (atomics, integers), backed by a huge-page-advised mapping.
pub struct ZeroVec<T> {
    _region: Region,
    ptr: *const T,
    len: usize,
}
unsafe impl<T: Sync> Send for ZeroVec<T> {}
unsafe impl<T: Sync> Sync for ZeroVec<T> {}

impl<T> ZeroVec<T> {
    pub fn new(len: usize) -> Self {
        let region = Region::new(len * std::mem::size_of::<T>());
        let ptr = region.ptr() as *const T;
        ZeroVec { _region: region, ptr, len }
    }
}

impl<T> std::ops::Deref for ZeroVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_roundtrip() {
        let pool = Pool::new(1 << 16); // small: forces the fallback regions too
        let mut a: Scatter<[u64; 3]> = Scatter::new(5);
        let mut b: Scatter<u64> = Scatter::new(3);
        let mut want: Vec<Vec<u64>> = vec![Vec::new(); 5];
        for i in 0..20_000u64 {
            let p = (i * 7 % 5) as usize;
            if p != 3 {
                let at = a.push(&pool, p, [i, i + 1, i + 2]);
                assert_eq!(unsafe { (*at)[0] }, i);
                want[p].push(i);
            }
        }
        for i in 0..700_000u64 {
            b.push(&pool, (i % 3) as usize, i);
        }
        for p in 0..5 {
            let got: Vec<u64> = a.blocks(p).flatten().map(|r| r[0]).collect();
            assert_eq!(got, want[p]);
            assert_eq!(a.len(p), want[p].len());
            assert!(a.blocks(p).flatten().all(|r| r[1] == r[0] + 1 && r[2] == r[0] + 2));
        }
        assert!(!pool.extra.lock().unwrap().0.is_empty());
        assert_eq!(b.total(), 700_000);
        assert_eq!(b.blocks(1).flatten().copied().collect::<Vec<_>>(), (0..700_000).filter(|i| i % 3 == 1).collect::<Vec<_>>());
    }
}
