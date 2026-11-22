//! Parquet writers for the two output tables.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::errors::Result;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::{Ctx, Store, URow};

const USER_SCHEMA: &str = "message user_daily {
  required binary user_id (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 events;
  required int64 revenue_cents;
  optional binary top_category (UTF8);
}";

const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
  required binary campaign_id (UTF8);
  required binary channel (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 purchasing_sessions;
  required int64 revenue_cents;
  required int64 assisted_sessions;
}";

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write(path: &Path, schema: &str, cols: Vec<Col>, rows: usize) -> Result<()> {
    let schema = Arc::new(parse_message_type(schema)?);
    let file = File::create(path)?;
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    if rows > 0 {
        let mut rg = w.next_row_group()?;
        for col in cols {
            let mut c = rg.next_column()?.expect("column");
            match col {
                Col::Bytes(v, def) => {
                    c.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None)?;
                }
                Col::I32(v) => {
                    c.typed::<Int32Type>().write_batch(&v, None, None)?;
                }
                Col::I64(v) => {
                    c.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
            }
            c.close()?;
        }
        rg.close()?;
    }
    w.close()?;
    Ok(())
}

pub fn write_user_daily(path: &Path, rows: &[URow], store: &Store, categories: &[Vec<u8>]) -> Result<()> {
    let names: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(store.get(r.name).to_vec())).collect();
    let cats: Vec<ByteArray> = categories.iter().map(|c| ByteArray::from(c.clone())).collect();
    let mut top = Vec::new();
    let mut def = Vec::with_capacity(rows.len());
    for r in rows {
        if r.top == u32::MAX {
            def.push(0i16);
        } else {
            def.push(1);
            top.push(cats[r.top as usize].clone());
        }
    }
    let cols = vec![
        Col::Bytes(names, None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions as i64).collect()),
        Col::I64(rows.iter().map(|r| r.events as i64).collect()),
        Col::I64(rows.iter().map(|r| r.revenue as i64).collect()),
        Col::Bytes(top, Some(def)),
    ];
    write(path, USER_SCHEMA, cols, rows.len())
}

pub fn write_campaign_daily(path: &Path, acc: &[[u64; 4]], ctx: &Ctx, ncamp: usize) -> Result<()> {
    let ndays = ctx.ndays;
    let mut id = Vec::new();
    let mut ch = Vec::new();
    let mut day = Vec::new();
    let mut c0 = Vec::new();
    let mut c1 = Vec::new();
    let mut c2 = Vec::new();
    let mut c3 = Vec::new();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = acc[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == ncamp {
                id.push(ByteArray::from("organic"));
                ch.push(ByteArray::from("organic"));
            } else {
                id.push(ByteArray::from(ctx.campaign_ids[c].clone()));
                ch.push(ByteArray::from(ctx.channels[c].clone()));
            }
            day.push((ctx.day0 + d as i64) as i32);
            c0.push(a[0] as i64);
            c1.push(a[1] as i64);
            c2.push(a[2] as i64);
            c3.push(a[3] as i64);
        }
    }
    let n = id.len();
    let cols = vec![
        Col::Bytes(id, None),
        Col::Bytes(ch, None),
        Col::I32(day),
        Col::I64(c0),
        Col::I64(c1),
        Col::I64(c2),
        Col::I64(c3),
    ];
    write(path, CAMPAIGN_SCHEMA, cols, n)
}
