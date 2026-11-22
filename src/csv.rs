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
