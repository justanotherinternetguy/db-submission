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


/// Per 64-byte block: bit i set where byte i is a separator (`,` or `\n`), a
/// newline, or a quote.
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
        let comma = _mm256_set1_epi8(b',' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
        let n = m(lo, nl) | (m(hi, nl) << 32);
        let c = m(lo, comma) | (m(hi, comma) << 32);
        let q = m(lo, quote) | (m(hi, quote) << 32);
        (c | n, n, q)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut s, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &c) in b.iter().enumerate() {
            s |= ((c == b',' || c == b'\n') as u64) << i;
            n |= ((c == b'\n') as u64) << i;
            q |= ((c == b'"') as u64) << i;
        }
        (s, n, q)
    }
}

/// Calls `emit` with the `N` unquoted fields of every well-formed line in
/// `data` (lines end at `\n`; the last may be unterminated). Malformed lines
/// (wrong field count, a quote anywhere but around a whole field, empty lines)
/// are skipped. Same dialect as `split`.
#[inline(always)]
pub fn scan<'a, const N: usize>(data: &'a [u8], mut emit: impl FnMut(&[&'a [u8]; N])) {
    let n = data.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fstart = 0usize;
    let mut qcarry = 0u32;
    let mut bs = 0usize;
    let mut tail = [0u8; 64];
    while bs < n {
        let block: &[u8; 64] = if bs + 64 <= n {
            data[bs..bs + 64].try_into().unwrap()
        } else {
            tail = [0u8; 64];
            tail[..n - bs].copy_from_slice(&data[bs..]);
            &tail
        };
        let (sep, nl, quo) = masks(block);
        let mut bits = sep;
        let mut fs_off = fstart.saturating_sub(bs); // 0..=64
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let p = bs + i;
            let qm = (quo & ((1u64 << i) - 1)) >> fs_off.min(63) << fs_off.min(63);
            let qc = qcarry + if fs_off >= 64 { 0 } else { qm.count_ones() };
            if nf < N {
                let f = &data[fstart..p];
                fields[nf] = if qc == 0 {
                    f
                } else if qc == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                    &f[1..f.len() - 1]
                } else {
                    bad = true;
                    f
                };
            } else {
                bad = true;
            }
            nf += 1;
            if (nl >> i) & 1 != 0 {
                if !bad && nf == N {
                    emit(&fields);
                }
                nf = 0;
                bad = false;
            }
            fstart = p + 1;
            fs_off = i + 1;
            qcarry = 0;
        }
        if fs_off < 64 {
            qcarry += (quo >> fs_off).count_ones();
        }
        bs += 64;
    }
    // An unterminated last line.
    if fstart < n || nf > 0 {
        let qc = qcarry;
        let p = n;
        if nf < N {
            let f = &data[fstart..p];
            fields[nf] = if qc == 0 {
                f
            } else if qc == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                &f[1..f.len() - 1]
            } else {
                bad = true;
                f
            };
        } else {
            bad = true;
        }
        nf += 1;
        if !bad && nf == N {
            emit(&fields);
        }
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
        scan::<N>(data, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
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
