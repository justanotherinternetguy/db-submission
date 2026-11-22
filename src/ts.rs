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

/// A direct-mapped per-thread memo of dates: `YYYY-MM-DD` -> days since the
/// epoch. Only valid dates are stored, so a hit also validates the date.
#[derive(Clone, Copy)]
struct DateSlot {
    k: u64, // bytes 2..10, "YY-MM-DD"
    c: u32, // bytes 0..2, "YY"
    days: i32,
}

const VALID: u32 = 1 << 16;

thread_local! {
    static DATES: std::cell::UnsafeCell<[DateSlot; 256]> =
        const { std::cell::UnsafeCell::new([DateSlot { k: 0, c: 0, days: 0 }; 256]) };
}

#[inline(always)]
fn load2(s: &[u8], i: usize) -> u32 {
    u16::from_le_bytes(s[i..i + 2].try_into().unwrap()) as u32
}

/// Days since the epoch of the date in `s[0..10]` (`YYYY-MM-DD`), if valid.
#[inline(always)]
fn date(s: &[u8]) -> Option<i64> {
    let k = load8(s, 2);
    let c = load2(s, 0);
    let i = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize;
    DATES.with(|m| {
        let e = unsafe { (*m.get()).get_unchecked(i) };
        if e.k == k && e.c == c | VALID {
            Some(e.days as i64)
        } else {
            date_miss(s, i, m)
        }
    })
}

#[cold]
#[inline(never)]
fn date_miss(s: &[u8], i: usize, m: &std::cell::UnsafeCell<[DateSlot; 256]>) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    unsafe { (*m.get())[i] = DateSlot { k: load8(s, 2), c: load2(s, 0) | VALID, days: days as i32 } };
    Some(days)
}

/// True if no byte of `y` exceeds its limit in `max` (bytes <= 0x7F).
#[inline(always)]
fn within(y: u64, max: u64) -> bool {
    let t = y.wrapping_add(0x7F7F_7F7F_7F7F_7F7F - max);
    (y | t) & 0x8080_8080_8080_8080 == 0
}

/// Seconds of the day of `HH:MM:SS` at `s[11..19]`, if valid.
#[inline(always)]
fn time_of_day(s: &[u8]) -> Option<i64> {
    // Digits become 0..9, colons 0.
    let y = load8(s, 11) ^ u64::from_le_bytes(*b"00:00:00");
    let ok = within(y, u64::from_le_bytes([2, 9, 0, 5, 9, 0, 5, 9]));
    let t = y.wrapping_mul(10).wrapping_add(y >> 8); // pairs in bytes 0, 3, 6
    let h = t & 0xFF;
    let mi = (t >> 24) & 0xFF;
    let se = (t >> 48) & 0xFF;
    if ok & (h <= 23) {
        Some((h * 3600 + mi * 60 + se) as i64)
    } else {
        None
    }
}

/// Minutes of a `+HH:MM` / `-HH:MM` offset at `s[19..25]`, signed so that
/// UTC = local - offset.
#[inline(always)]
fn offset(s: &[u8]) -> Option<i64> {
    // bytes 17..25: "SS+HH:MM"; check bytes 3..8.
    let w = load8(s, 17);
    let y = (w ^ u64::from_le_bytes(*b"\0\0\000:00")) & !0xFF_FFFF;
    let ok = within(y, u64::from_le_bytes([0, 0, 0, 2, 9, 0, 5, 9]));
    let t = y.wrapping_mul(10).wrapping_add(y >> 8); // pairs in bytes 3, 6
    let oh = (t >> 24) & 0xFF;
    let om = (t >> 48) & 0xFF;
    let sign = s[19];
    let m = (oh * 60 + om) as i64;
    if ok & (oh <= 23) {
        match sign {
            b'+' => Some(m),
            b'-' => Some(-m),
            _ => None,
        }
    } else {
        None
    }
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

/// F1 only (`YYYY-MM-DDTHH:MM:SSZ`), for `ingest_ts`; None for anything else.
#[inline(always)]
pub fn parse_f1(s: &[u8]) -> Option<i64> {
    if s.len() == 20 && s[10] == b'T' && s[19] == b'Z' {
        let tod = time_of_day(s)?;
        let d = date(s)?;
        Some((d * 86_400 + tod) * 1000)
    } else {
        None
    }
}

/// Milliseconds since the epoch for F1, F2 or F3; None for anything else.
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        13 => {
            let a = load8(s, 0);
            let b = load8(s, 5);
            if digits8(a) & digits8(b) {
                Some((value8(a) * 100_000 + value8(b) % 100_000) as i64)
            } else {
                None
            }
        }
        20 if s[19] == b'Z' => parse_f1(s),
        25 if s[10] == b'T' => {
            let tod = time_of_day(s)?;
            let off = offset(s)?;
            let d = date(s)?;
            Some((d * 86_400 + tod - off * 60) * 1000)
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
            let f1 = if v.len() == 20 && v[19] == b'Z' { parse_slow(&v) } else { None };
            assert_eq!(super::parse_f1(&v), f1, "{:?}", String::from_utf8_lossy(&v));
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
