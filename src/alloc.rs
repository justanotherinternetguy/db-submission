//! The system allocator for small blocks; blocks of 1 MiB or more get their
//! own anonymous mapping, rounded to 2 MiB and advised for transparent huge
//! pages. With THP in "madvise" mode (the scorer's), big heap buffers would
//! otherwise fault in, and be torn down at exit, one 4 KiB page at a time.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

const HUGE: usize = 2 << 20;
/// Blocks at least this big are mapped directly.
const BIG: usize = 1 << 20;

#[inline]
fn big(layout: &Layout) -> bool {
    layout.size() >= BIG && layout.align() <= 4096
}

#[inline]
fn map_len(size: usize) -> usize {
    size.div_ceil(HUGE) * HUGE
}

unsafe fn map(size: usize) -> *mut u8 {
    let len = map_len(size);
    let p = libc::mmap(
        std::ptr::null_mut(),
        len,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
        -1,
        0,
    );
    if p == libc::MAP_FAILED {
        return std::ptr::null_mut();
    }
    libc::madvise(p, len, libc::MADV_HUGEPAGE);
    p as *mut u8
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if big(&layout) {
            map(layout.size())
        } else {
            System.alloc(layout)
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if big(&layout) {
            map(layout.size()) // fresh mappings are zero
        } else {
            System.alloc_zeroed(layout)
        }
    }

    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if big(&layout) {
            libc::munmap(p as *mut libc::c_void, map_len(layout.size()));
        } else {
            System.dealloc(p, layout)
        }
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
        match (big(&layout), big(&new_layout)) {
            (false, false) => System.realloc(p, layout, new_size),
            (true, true) => {
                let (old_len, new_len) = (map_len(layout.size()), map_len(new_size));
                if old_len == new_len {
                    return p;
                }
                let q = libc::mremap(p as *mut libc::c_void, old_len, new_len, libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(q, new_len, libc::MADV_HUGEPAGE);
                q as *mut u8
            }
            _ => {
                let q = self.alloc(new_layout);
                if !q.is_null() {
                    std::ptr::copy_nonoverlapping(p, q, layout.size().min(new_size));
                    self.dealloc(p, layout);
                }
                q
            }
        }
    }
}
