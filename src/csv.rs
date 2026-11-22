//! The CSV dialect of rules.md: `,` separates fields (values never hold commas
//! or quotes), a field may be wrapped whole in `"`, an empty field is null, and
//! any other quote or a wrong field count makes the line malformed.

/// Splits `line` into exactly `N` unquoted fields. Returns false if malformed.
#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let n = line.len();
    let mut field = 0usize;
    let mut start = 0usize;
    loop {
        // Find the end of this field and whether it contains a quote.
        let mut j = start;
        let mut quotes = 0u32;
        while j < n {
            let c = line[j];
            if c == b',' {
                break;
            }
            quotes += (c == b'"') as u32;
            j += 1;
        }
        if field >= N {
            return false;
        }
        let f = &line[start..j];
        out[field] = if quotes == 0 {
            f
        } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
            &f[1..f.len() - 1]
        } else {
            return false;
        };
        field += 1;
        if j >= n {
            break;
        }
        start = j + 1;
    }
    field == N
}

/// Value of the 1..=8 ASCII digits ending at `end` (exclusive), or None if any
/// is not a digit. Reads the 8 bytes before `end`.
///
/// # Safety
/// `end - 8 .. end` must be readable.
#[inline(always)]
pub unsafe fn digits_tail(end: *const u8, len: usize) -> Option<u64> {
    debug_assert!((1..=8).contains(&len));
    let w = (end.sub(8) as *const u64).read_unaligned();
    let low = (8 - len) * 8; // bits before the field, replaced by '0'
    let mask = if low == 0 { 0 } else { (1u64 << low) - 1 };
    let w = (w & !mask) | (0x3030_3030_3030_3030 & mask);
    if crate::ts::digits8(w) {
        Some(crate::ts::value8(w))
    } else {
        None
    }
}

/// `price`, reading up to 8 bytes before the field when `room` (at least 8
/// readable bytes end at the field's end).
#[inline(always)]
pub fn price_fast(s: &[u8], room: bool) -> Option<u32> {
    if room && (1..=8).contains(&s.len()) {
        let v = unsafe { digits_tail(s.as_ptr().add(s.len()), s.len()) }?;
        return if (1..=10_000_000).contains(&v) { Some(v as u32) } else { None };
    }
    price(s)
}

/// A purchase price: ASCII digits only, value 1 to 10,000,000.
#[inline]
pub fn price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn sp<const N: usize>(s: &str) -> Option<Vec<String>> {
        let mut o: [&[u8]; N] = [&[]; N];
        if split::<N>(s.as_bytes(), &mut o) {
            Some(o.iter().map(|x| String::from_utf8(x.to_vec()).unwrap()).collect())
        } else {
            None
        }
    }

    #[test]
    fn dialect() {
        assert_eq!(sp::<2>("a,b"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(sp::<2>("\"a\",\"\""), Some(vec!["a".into(), "".into()]));
        assert_eq!(sp::<2>("a,"), Some(vec!["a".into(), "".into()]));
        assert_eq!(sp::<2>(""), None);
        assert_eq!(sp::<2>("a"), None);
        assert_eq!(sp::<2>("a,b,c"), None);
        assert_eq!(sp::<2>("a,b\""), None);
        assert_eq!(sp::<2>("\"a,b"), None);
        assert_eq!(sp::<2>("\",b"), None);
        assert_eq!(sp::<2>("a\"x\",b"), None);
        assert_eq!(sp::<2>("\"a\"\"\",b"), None);
        assert_eq!(sp::<2>(" a , b\r"), Some(vec![" a ".into(), " b\r".into()]));
    }

    fn scan_all<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        let mut buf = Vec::new();
        scan::<N>(data, &mut buf, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    fn split_all<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        let mut lines: Vec<&[u8]> = data.split(|&c| c == b'\n').collect();
        if data.last() == Some(&b'\n') {
            lines.pop();
        }
        for line in lines {
            let mut o: [&[u8]; N] = [&[]; N];
            if split::<N>(line, &mut o) {
                v.push(o.iter().map(|x| x.to_vec()).collect());
            }
        }
        v
    }

    #[test]
    fn scan_matches_split() {
        // Deterministic pseudo-random lines over a small alphabet heavy in structure.
        let alphabet = b"ab,\"\n \rx";
        let mut state = 0x1234_5678_9abc_def0u64;
        for round in 0..20000 {
            let len = (round % 300) as usize;
            let mut data = Vec::with_capacity(len);
            for _ in 0..len {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                data.push(alphabet[((state >> 33) % alphabet.len() as u64) as usize]);
            }
            assert_eq!(scan_all::<2>(&data), split_all::<2>(&data), "{:?}", String::from_utf8_lossy(&data));

            assert_eq!(scan_all::<3>(&data), split_all::<3>(&data), "{:?}", String::from_utf8_lossy(&data));
        }
        // Long fields crossing many blocks.
        let mut data = Vec::new();
        for k in 0..50 {
            data.extend(std::iter::repeat(b'a').take(k * 37));
            data.extend_from_slice(b",\"");
            data.extend(std::iter::repeat(b'b').take(k * 13));
            data.extend_from_slice(b"\"\n");
        }
        assert_eq!(scan_all::<2>(&data), split_all::<2>(&data));
        assert_eq!(scan_all::<2>(&data).len(), 50);
        // Many sub-chunks, malformed lines among them.
        let mut big = Vec::new();
        for k in 0..40_000u32 {
            match k % 7 {
                0 => big.extend_from_slice(b"a,\"b\",c\n"),
                1 => big.extend_from_slice(b"a,b\n"),
                2 => big.extend_from_slice(b"\n"),
                3 => big.extend_from_slice(b"\"x\",\"\",z\n"),
                4 => big.extend_from_slice(b"q,w,e,r\n"),
                5 => big.extend_from_slice(b"1,2\"\",3\n"),
                _ => big.extend_from_slice(format!("{k},{k},{k}\n").as_bytes()),
            }
        }
        big.extend_from_slice(b"end,no,newline");
        assert_eq!(scan_all::<3>(&big), split_all::<3>(&big));
    }

    #[test]
    fn prices_fast() {
        // Each candidate placed after 16 bytes of junk so the tail read is in bounds.
        let cands: Vec<&[u8]> = vec![b"0250", b"1", b"0", b"00", b"10000000", b"10000001", b"99999999", b"12a4",
            b"", b"+5", b"1000\r", b"0000000250", b"00000001", b"7", b"\xef\xbc\x92"];
        for c in cands {
            let mut v = b"0123456789,9,\"x,".to_vec();
            v.extend_from_slice(c);
            let f = &v[16..];
            assert_eq!(price_fast(f, true), price(f), "{:?}", String::from_utf8_lossy(c));
        }
    }

    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"00"), None);
        assert_eq!(price(b"1000\r"), None);
        assert_eq!(price(b"+5"), None);
        assert_eq!(price(b"000000000000000000000000001"), Some(1));
        assert_eq!(price(b"99999999999999999999999999"), None);
    }
}

/// Calls `emit` with the `N` unquoted fields of every well-formed line in
/// `data` (lines end at `\n`; the last may be unterminated). Malformed lines
/// (wrong field count, a quote anywhere but around a whole field, empty lines)
/// are skipped: the same dialect as `split`. `buf` is scratch space.
#[inline(always)]
pub fn scan<'a, const N: usize>(data: &'a [u8], buf: &mut Vec<u64>, mut emit: impl FnMut(&[&'a [u8]; N])) {
    // The first line goes through `split`, so that a readable byte precedes
    // every block (`scan_block` reads the byte before a field's end).
    let n = data.len();
    let first = match memchr::memchr(b'\n', data) {
        Some(i) => i,
        None => n,
    };
    let mut f: [&'a [u8]; N] = [&[]; N];
    if n > 0 && split::<N>(&data[..first], &mut f) {
        emit(&f);
    }
    // Sub-chunks of about 64 KiB cut at newlines keep `buf` in L2.
    let mut s = first + 1;
    while s < n {
        let mut e = (s + (64 << 10)).min(n);
        if e < n {
            e = memchr::memchr(b'\n', &data[e..]).map_or(n, |i| e + i + 1);
        }
        scan_block::<N>(&data[s..e], buf, &mut emit);
        s = e;
    }
}

/// Writes the positions `base + i` of the set bits `i` of `bits` to `out`,
/// W at a time (up to W - 1 slots past the count may be written); returns the count.
#[inline(always)]
unsafe fn flatten<const W: usize>(out: *mut u32, base: u32, mut bits: u64) -> usize {
    let cnt = bits.count_ones() as usize;
    let mut k = 0;
    loop {
        for j in 0..W {
            *out.add(k + j) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
        }
        k += W;
        if k >= cnt {
            break;
        }
    }
    cnt
}

/// Two-stage scanner. Stage 1 records, per 64-byte block, the comma
/// positions, the newline positions, and the quote mask with the running quote
/// count before the block. Stage 2 walks lines: a line is well-formed when
/// exactly `N - 1` commas precede its newline and its quote count is twice the
/// number of fields that are wrapped whole in quotes (every quote is then one
/// of those wrapping pairs). `data.len()` must be < 2^31, and the byte
/// before `data` must be readable.
#[inline(always)]
fn scan_block<'a, const N: usize>(data: &'a [u8], buf: &mut Vec<u64>, emit: &mut impl FnMut(&[&'a [u8]; N])) {
    let n = data.len();
    if n == 0 {
        return;
    }
    debug_assert!(n < (1 << 31));
    let nb = n.div_ceil(64);
    // u64 words: commas (n + 64 u32s, + N sentinels), newlines (same), quote masks (nb), counts (nb).
    let sep_cap = n + 64 + 2 * N;
    let words = sep_cap + 2 * nb + 4;
    buf.clear();
    buf.reserve(words);
    let base = buf.as_mut_ptr();
    let com = base as *mut u32;
    let nls = unsafe { com.add(sep_cap) };
    let qm = unsafe { base.add(sep_cap) }; // after 2 * sep_cap u32s
    let qc = unsafe { qm.add(nb) as *mut u32 };
    let mut ncom = 0usize;
    let mut nnl = 0usize;
    let mut cq = 0u32;
    let mut tail = [0u8; 64];
    let mut b = 0usize;
    while b < nb {
        let bs = b * 64;
        let block: &[u8; 64] = if bs + 64 <= n {
            unsafe { &*(data.as_ptr().add(bs) as *const [u8; 64]) }
        } else {
            tail[..n - bs].copy_from_slice(&data[bs..]);
            tail[n - bs..].fill(0);
            &tail
        };
        let (c, nl, q) = masks3(block);
        unsafe {
            *qm.add(b) = q;
            *qc.add(b) = cq;
            cq += q.count_ones();
            ncom += flatten::<8>(com.add(ncom), bs as u32, c);
            nnl += flatten::<2>(nls.add(nnl), bs as u32, nl);
        }
        b += 1;
    }
    // Sentinels: no line has a comma at or after u32::MAX.
    for j in 0..N {
        unsafe { *com.add(ncom + j) = u32::MAX };
    }
    let com = unsafe { std::slice::from_raw_parts(com as *const u32, ncom + N) };
    let nls = unsafe { std::slice::from_raw_parts(nls as *const u32, nnl) };
    let quotes_before = |p: usize| unsafe {
        let w = *qm.add(p >> 6);
        let below = w & ((1u64 << (p & 63)) - 1);
        *qc.add(p >> 6) + below.count_ones()
    };

    let dp = data.as_ptr();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut c = 0usize; // first comma after the line start
    let mut ls = 0usize; // line start
    let mut qprev = 0u32; // quotes before the line start
    for &le in nls {
        let le = le as usize;
        let qnow = quotes_before(le);
        let ok_count = N == 1 || (unsafe { *com.get_unchecked(c + N - 2) } as usize) < le;
        let next = unsafe { *com.get_unchecked(c + N - 1) } as usize;
        if ok_count && next > le {
            let mut start = ls;
            let mut wrapped = 0u32;
            for j in 0..N {
                let end = if j + 1 < N { (unsafe { *com.get_unchecked(c + j) }) as usize } else { le };
                // Branch-free: whether a field is quoted is unpredictable.
                // start < n; the byte before `data` is readable (see `scan`).
                let first = unsafe { *dp.add(start) };
                let last = unsafe { *dp.add(end).sub(1) };
                let w = (first == b'"') & (last == b'"') & (end >= start + 2);
                wrapped += w as u32;
                let q = w as usize;
                fields[j] = unsafe { std::slice::from_raw_parts(dp.add(start + q), end - start - 2 * q) };
                start = end + 1;
            }
            if qnow - qprev == 2 * wrapped {
                emit(&fields);
            }
            c += N - 1;
        } else {
            while (unsafe { *com.get_unchecked(c) } as usize) < le {
                c += 1;
            }
        }
        qprev = qnow;
        ls = le + 1;
    }
    // An unterminated last line.
    if ls < n {
        let mut f: [&'a [u8]; N] = [&[]; N];
        if split::<N>(&data[ls..], &mut f) {
            emit(&f);
        }
    }
}

/// Per 64-byte block: bit i set where byte i is a comma, a newline, a quote.
#[inline(always)]
fn masks3(b: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
        let m = |c: u8| {
            let c = _mm256_set1_epi8(c as i8);
            let l = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, c)) as u32 as u64;
            let h = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, c)) as u32 as u64;
            l | (h << 32)
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &x) in b.iter().enumerate() {
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}
