//! The system allocator, with transparent huge pages asked for on every large
//! block. With THP in "madvise" mode (the scorer's), big heap buffers would
//! otherwise fault in and be torn down at exit one 4 KiB page at a time.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

const HUGE: usize = 2 << 20;
/// Blocks at least this big always contain a whole aligned huge page.
const MIN: usize = 2 * HUGE;

#[inline]
fn advise(p: *mut u8, size: usize) {
    if size >= MIN && !p.is_null() {
        let start = (p as usize).div_ceil(HUGE) * HUGE;
        let end = (p as usize + size) & !(HUGE - 1);
        if end > start {
            unsafe { libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE) };
        }
    }
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        advise(p, layout.size());
        p
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        advise(p, layout.size());
        p
    }

    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout)
    }

    #[inline]
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let q = System.realloc(p, layout, new_size);
        advise(q, new_size);
        q
    }
}
