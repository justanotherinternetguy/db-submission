//! The two output tables, written with the minimal Parquet writer in pq.rs.

use std::path::Path;

use crate::pq::{write, Column, Kind};
use crate::{Ctx, Store, URow};

pub fn write_user_daily(path: &Path, rows: &[URow], store: &Store, categories: &[Vec<u8>]) -> std::io::Result<()> {
    let mut user = Column::new("user_id", Kind::Str, false);
    let mut day = Column::new("day", Kind::Date, false);
    let mut sessions = Column::new("sessions", Kind::Int64, false);
    let mut events = Column::new("events", Kind::Int64, false);
    let mut revenue = Column::new("revenue_cents", Kind::Int64, false);
    let mut top = Column::new("top_category", Kind::Str, true);
    user.data.reserve(rows.len() * 16);
    for c in [&mut day] {
        c.data.reserve(rows.len() * 4);
    }
    for c in [&mut sessions, &mut events, &mut revenue] {
        c.data.reserve(rows.len() * 8);
    }
    top.defined.reserve(rows.len());
    for r in rows {
        user.put_str(store.get(r.name));
        day.put_i32(r.day);
        sessions.put_i64(r.sessions as i64);
        events.put_i64(r.events as i64);
        revenue.put_i64(r.revenue as i64);
        if r.top == u32::MAX {
            top.put_null();
        } else {
            top.put_str(&categories[r.top as usize]);
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
