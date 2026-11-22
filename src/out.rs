//! The two output tables, written with the minimal Parquet writer in pq.rs.

use std::path::Path;

use crate::pq::{write, Column, Kind};
use crate::{Ctx, Store, URow};

/// A non-negative integer column at the smallest encoding that holds every
/// value: a dictionary of 0..=max (the value is its own index) when max is
/// small, else PLAIN INT32, else PLAIN INT64.
fn int_column(name: &'static str, rows: &[URow], f: impl Fn(&URow) -> u64) -> Column {
    let max = rows.iter().map(&f).max().unwrap_or(0);
    if max < 1 << 16 && (max as usize) < rows.len() {
        let mut c = Column::dictionary(name, Kind::Int32, false);
        for v in 0..=max {
            c.entry_i32(v as i32);
        }
        c.idx.extend(rows.iter().map(|r| f(r) as u32));
        c
    } else if max <= i32::MAX as u64 {
        let mut c = Column::new(name, Kind::Int32, false);
        c.data.reserve(rows.len() * 4);
        for r in rows {
            c.put_i32(f(r) as i32);
        }
        c
    } else {
        let mut c = Column::new(name, Kind::Int64, false);
        c.data.reserve(rows.len() * 8);
        for r in rows {
            c.put_i64(f(r) as i64);
        }
        c
    }
}

pub fn write_user_daily(path: &Path, rows: &[URow], store: &Store, categories: &[Vec<u8>]) -> std::io::Result<()> {
    // A person's rows are adjacent: each name enters the dictionary once.
    let mut user = Column::dictionary("user_id", Kind::Str, false);
    user.data.reserve(rows.len() * 8);
    user.idx.reserve(rows.len());
    let mut last = u64::MAX;
    for r in rows {
        if r.name != last {
            last = r.name;
            user.entry_str(store.get_name(r.name));
        }
        user.idx.push(user.dict.unwrap() as u32 - 1);
    }
    let d0 = rows.iter().map(|r| r.day).min().unwrap_or(0);
    let d1 = rows.iter().map(|r| r.day).max().unwrap_or(0);
    let mut day = Column::dictionary("day", Kind::Date, false);
    for d in d0..=d1 {
        day.entry_i32(d);
    }
    day.idx.extend(rows.iter().map(|r| (r.day - d0) as u32));
    let sessions = int_column("sessions", rows, |r| r.sessions as u64);
    let events = int_column("events", rows, |r| r.events as u64);
    let revenue = int_column("revenue_cents", rows, |r| r.revenue);
    let mut top = Column::dictionary("top_category", Kind::Str, true);
    for c in categories {
        top.entry_str(c);
    }
    top.idx.reserve(rows.len());
    top.defined.reserve(rows.len());
    for r in rows {
        if r.top == u32::MAX {
            top.put_null();
        } else {
            top.put_idx(r.top);
        }
    }
    write(path, "user_daily", &[user, day, sessions, events, revenue, top], rows.len())
}

pub fn write_campaign_daily(path: &Path, acc: &[[u64; 4]], ctx: &Ctx, ncamp: usize) -> std::io::Result<()> {
    let ndays = ctx.ndays;
    let mut id = Column::new("campaign_id", Kind::Str, false);
    let mut ch = Column::new("channel", Kind::Str, false);
    let mut day = Column::new("day", Kind::Date, false);
    let mut c0 = Column::new("sessions", Kind::Int64, false);
    let mut c1 = Column::new("purchasing_sessions", Kind::Int64, false);
    let mut c2 = Column::new("revenue_cents", Kind::Int64, false);
    let mut c3 = Column::new("assisted_sessions", Kind::Int64, false);
    let mut n = 0;
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = acc[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == ncamp {
                id.put_str(b"organic");
                ch.put_str(b"organic");
            } else {
                id.put_str(&ctx.campaign_ids[c]);
                ch.put_str(&ctx.channels[c]);
            }
            day.put_i32((ctx.day0 + d as i64) as i32);
            c0.put_i64(a[0] as i64);
            c1.put_i64(a[1] as i64);
            c2.put_i64(a[2] as i64);
            c3.put_i64(a[3] as i64);
            n += 1;
        }
    }
    write(path, "campaign_daily", &[id, ch, day, c0, c1, c2, c3], n)
}
