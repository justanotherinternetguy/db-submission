//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 epoch
//! milliseconds as ASCII digits. Returns milliseconds since the epoch, or None
//! for anything else (including F3 values too large for an i64).

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some((a * 10 + b) as i64)
    }
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

/// The straightforward parser; `parse` must agree with it on every input.
#[inline]
pub fn parse_slow(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    let c0 = s[0].wrapping_sub(b'0');
    if c0 <= 9 && (n != 20 && n != 25 || s.iter().all(|c| c.is_ascii_digit())) {
        // F3 (a 20- or 25-byte string can only be F3 if it is all digits).
        let mut v: u64 = 0;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v.checked_mul(10)?.checked_add(d as u64)?;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let offset_min = if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se - offset_min * 60;
    Some(secs * 1000)
}


#[inline(always)]
fn load8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// True if all 8 bytes are ASCII digits.
#[inline(always)]
pub fn digits8(w: u64) -> bool {
    ((w & 0xF0F0_F0F0_F0F0_F0F0) | ((w.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) >> 4))
        == 0x3333_3333_3333_3333
}

/// Value of 8 ASCII digits (first digit in the low byte).
#[inline(always)]
pub fn value8(w: u64) -> u64 {
    let w = w.wrapping_sub(0x3030_3030_3030_3030);
    let w = (w.wrapping_mul(10)).wrapping_add(w >> 8);
    let a = (w & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32));
    let b = ((w >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32));
    (a.wrapping_add(b)) >> 32
}

/// Replace the bytes selected by `mask` (which must equal `expect`) with '0'.
#[inline(always)]
fn seps(w: u64, mask: u64, expect: u64) -> (bool, u64) {
    ((w & mask) == expect, (w & !mask) | (0x3030_3030_3030_3030 & mask))
}

#[inline(always)]
fn byte(w: u64, i: u32) -> i64 {
    ((w >> (8 * i)) & 0xFF) as i64 - 0x30
}

const B: [u64; 8] = [0xFF, 0xFF << 8, 0xFF << 16, 0xFF << 24, 0xFF << 32, 0xFF << 40, 0xFF << 48, 0xFF << 56];

#[inline(always)]
fn rep(c: u8, i: usize) -> u64 {
    (c as u64) << (8 * i)
}

/// Fields shared by F1 and F2: returns (ok, seconds since epoch of the local time).
#[inline(always)]
fn civil_part(s: &[u8]) -> (bool, i64) {
    // "YYYY-MM-" "DDTHH:MM" ":SS" checked as three overlapping words.
    let (ok0, w0) = seps(load8(s, 0), B[4] | B[7], rep(b'-', 4) | rep(b'-', 7));
    let (ok1, w1) = seps(load8(s, 8), B[2] | B[5], rep(b'T', 2) | rep(b':', 5));
    // bytes 11..19: "HH:MM:SS"
    let (ok2, w2) = seps(load8(s, 11), B[2] | B[5], rep(b':', 2) | rep(b':', 5));
    let ok = ok0 & ok1 & ok2 & digits8(w0) & digits8(w1) & digits8(w2);
    let y = byte(w0, 0) * 1000 + byte(w0, 1) * 100 + byte(w0, 2) * 10 + byte(w0, 3);
    let mo = byte(w0, 5) * 10 + byte(w0, 6);
    let d = byte(w1, 0) * 10 + byte(w1, 1);
    let h = byte(w2, 0) * 10 + byte(w2, 1);
    let mi = byte(w2, 3) * 10 + byte(w2, 4);
    let se = byte(w2, 6) * 10 + byte(w2, 7);
    let ok = ok
        & (1..=12).contains(&mo)
        & (d >= 1)
        & (d <= days_in_month(y, mo.clamp(1, 12)))
        & (h <= 23)
        & (mi <= 59)
        & (se <= 59);
    (ok, days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se)
}

#[inline(always)]
fn f3_slow(s: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// Milliseconds since the epoch for F1, F2 or F3; None for anything else.
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 if s[4] == b'-' => {
            let (ok, secs) = civil_part(s);
            if ok & (s[19] == b'Z') {
                Some(secs * 1000)
            } else {
                None
            }
        }
        25 if s[4] == b'-' => {
            let (ok, secs) = civil_part(s);
            // bytes 17..25: "SS+HH:MM"
            let w = load8(s, 17);
            let sign = (w >> 16) & 0xFF;
            let (ok3, w3) = seps(w, B[2] | B[5], rep(b'0', 2) | rep(b':', 5));
            let _ = ok3;
            let colon = (w & B[5]) == rep(b':', 5);
            let w3 = (w3 & !B[2]) | rep(b'0', 2);
            let oh = byte(w3, 3) * 10 + byte(w3, 4);
            let om = byte(w3, 6) * 10 + byte(w3, 7);
            let ok = ok & colon & digits8(w3) & (oh <= 23) & (om <= 59) & ((sign == b'+' as u64) | (sign == b'-' as u64));
            if ok {
                let off = (oh * 60 + om) * 60;
                Some((if sign == b'+' as u64 { secs - off } else { secs + off }) * 1000)
            } else {
                None
            }
        }
        13 => {
            let a = load8(s, 0);
            let b = load8(s, 5);
            if digits8(a) & digits8(b) {
                Some((value8(a) * 100_000 + value8(b) % 100_000) as i64)
            } else {
                None
            }
        }
        0 => None,
        _ => f3_slow(s),
    }
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"00000001791071890000"), Some(f1)); // 20 digits
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"86400000"), Some(86_400_000));
        assert_eq!(parse(b"2028-02-29T00:00:00Z").is_some(), true);
    }

    #[test]
    fn fast_matches_slow() {
        use super::{days_from_civil, parse_slow};
        let seeds: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"2028-02-29T23:59:59+23:59",
            b"0000000000000", b"9999999999999", b"2026-12-31T00:00:00-00:00", b"1999-01-01T00:00:00Z",
            b"00000001791071890000", b"0000000001791071890000000", b"2027-02-28T12:00:00+05:30",
        ];
        let alphabet = b"0123456789-T:Z+ zt.e\xef\xbc\x92";
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 33
        };
        for round in 0..3_000_000u64 {
            let base = seeds[(round % seeds.len() as u64) as usize];
            let mut v = base.to_vec();
            let edits = next() % 4;
            for _ in 0..edits {
                let i = (next() as usize) % v.len().max(1);
                match next() % 4 {
                    0 if !v.is_empty() => v[i] = alphabet[(next() as usize) % alphabet.len()],
                    1 if !v.is_empty() => v[i] = b'0' + (next() % 10) as u8,
                    2 => v.insert(i.min(v.len()), alphabet[(next() as usize) % alphabet.len()]),
                    _ if !v.is_empty() => {
                        v.remove(i);
                    }
                    _ => {}
                }
            }
            assert_eq!(parse(&v), parse_slow(&v), "{:?}", String::from_utf8_lossy(&v));
        }
        // Every calendar day 1999..2031, and every offset.
        for days in days_from_civil(1999, 1, 1)..days_from_civil(2031, 1, 1) {
            let ms = days * 86_400_000 + 45_296_000;
            let secs = ms / 1000;
            let d = secs.div_euclid(86_400);
            // civil from days (Hinnant)
            let z = d + 719_468;
            let era = z.div_euclid(146_097);
            let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let day = doy - (153 * mp + 2) / 5 + 1;
            let month = if mp < 10 { mp + 3 } else { mp - 9 };
            let year = yoe + era * 400 + (month <= 2) as i64;
            let text = format!("{year:04}-{month:02}-{day:02}T12:34:56Z");
            assert_eq!(parse(text.as_bytes()), Some(ms), "{text}");
            assert_eq!(parse_slow(text.as_bytes()), Some(ms), "{text}");
            for off in [-600i64, -570, 0, 345, 720, 1439] {
                let sign = if off < 0 { '-' } else { '+' };
                let text = format!("{year:04}-{month:02}-{day:02}T12:34:56{sign}{:02}:{:02}", off.abs() / 60, off.abs() % 60);
                assert_eq!(parse(text.as_bytes()), parse_slow(text.as_bytes()), "{text}");
            }
        }
    }

    #[test]
    fn bad() {
        for s in [
            "",
            "2026-10-03T23:58:10.000Z",
            "2026-10-03T23:58:10z",
            "2026-10-03t23:58:10Z",
            "2026-10-03 23:58:10Z",
            "2026-10-03T23:58:10",
            "1791071890000.0",
            "1.79e12",
            "-5",
            "2026-13-03T23:58:10Z",
            "2026-02-30T23:58:10Z",
            "2027-02-29T23:58:10Z",
            "2026-10-03T24:00:00Z",
            "2026-10-03T23:58:60Z",
            "2026-10-03T23:58:10+24:00",
            "2026-10-03T23:58:10+05:60",
            "2026-10-03T23:58:10+0200",
            "2026-10-03T23:58:10Z\r",
            "999999999999999999999",
            "9223372036854775808",
            "２０２6-10-03T23:58:10Z",
        ] {
            assert_eq!(parse(s.as_bytes()), None, "{s:?}");
        }
    }
}
