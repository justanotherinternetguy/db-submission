//! Partitioned append buffers backed by one huge-page-advised anonymous
//! mapping: each partition gets a fixed-capacity region, with a Vec for overflow.

use std::marker::PhantomData;

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

pub struct Parts<T: Copy> {
    _region: Region,
    base: *mut T,
    cap: usize,
    lens: Vec<usize>,
    overflow: Vec<Vec<T>>,
    _t: PhantomData<T>,
}
unsafe impl<T: Copy + Send> Send for Parts<T> {}
unsafe impl<T: Copy + Sync> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    pub fn new(nparts: usize, cap: usize) -> Self {
        let cap = cap.max(4);
        let region = Region::new(nparts * cap * std::mem::size_of::<T>());
        assert!(region.len >= nparts * cap * std::mem::size_of::<T>());
        let base = region.ptr as *mut T;
        Parts { _region: region, base, cap, lens: vec![0; nparts], overflow: (0..nparts).map(|_| Vec::new()).collect(), _t: PhantomData }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, v: T) {
        let l = unsafe { *self.lens.get_unchecked(p) };
        if l < self.cap {
            unsafe {
                self.base.add(p * self.cap + l).write(v);
                *self.lens.get_unchecked_mut(p) = l + 1;
            }
        } else {
            self.overflow[p].push(v);
        }
    }

    #[inline]
    pub fn main(&self, p: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.base.add(p * self.cap), self.lens[p]) }
    }

    #[inline]
    pub fn extra(&self, p: usize) -> &[T] {
        &self.overflow[p]
    }

    #[inline]
    pub fn len(&self, p: usize) -> usize {
        self.lens[p] + self.overflow[p].len()
    }

    #[inline]
    pub fn get(&self, p: usize, i: usize) -> &T {
        let l = self.lens[p];
        if i < l {
            unsafe { &*self.base.add(p * self.cap + i) }
        } else {
            &self.overflow[p][i - l]
        }
    }

    pub fn iter(&self, p: usize) -> impl Iterator<Item = &T> {
        self.main(p).iter().chain(self.extra(p).iter())
    }

    pub fn total(&self) -> usize {
        (0..self.lens.len()).map(|p| self.len(p)).sum()
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
