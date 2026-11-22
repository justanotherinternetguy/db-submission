//! A minimal Parquet writer: one row group, one uncompressed v1 data page per
//! column (PLAIN, or RLE_DICTIONARY after a PLAIN dictionary page),
//! thrift-compact metadata. Enough for the two output tables.

use std::io::Write;

pub enum Kind {
    /// BYTE_ARRAY annotated as a UTF-8 string.
    Str,
    /// INT32 annotated as DATE.
    Date,
    Int32,
    Int64,
}

impl Kind {
    fn physical(&self) -> i32 {
        match self {
            Kind::Str => 6,                 // BYTE_ARRAY
            Kind::Date | Kind::Int32 => 1, // INT32
            Kind::Int64 => 2,              // INT64
        }
    }
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
    /// PLAIN-encoded values (present values only); for a dictionary column,
    /// the dictionary entries.
    pub data: Vec<u8>,
    /// For optional columns: one definition level (0/1) per row.
    pub defined: Vec<bool>,
    /// For a dictionary column: number of entries in `data`, and one entry
    /// index per present value.
    pub dict: Option<usize>,
    pub idx: Vec<u32>,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, optional: bool) -> Self {
        Column { name, kind, optional, data: Vec::new(), defined: Vec::new(), dict: None, idx: Vec::new() }
    }
    /// A dictionary-encoded column: add entries with `entry_*`, then values
    /// with `put_idx` (an entry's position) or `put_null`.
    pub fn dictionary(name: &'static str, kind: Kind, optional: bool) -> Self {
        Column { dict: Some(0), ..Column::new(name, kind, optional) }
    }
    #[inline]
    pub fn entry_str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
        *self.dict.as_mut().unwrap() += 1;
    }
    #[inline]
    pub fn entry_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
        *self.dict.as_mut().unwrap() += 1;
    }
    #[inline]
    pub fn put_idx(&mut self, i: u32) {
        self.idx.push(i);
        if self.optional {
            self.defined.push(true);
        }
    }
    #[inline]
    pub fn put_str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
        if self.optional {
            self.defined.push(true);
        }
    }
    #[inline]
    pub fn put_null(&mut self) {
        self.defined.push(false);
    }
    #[inline]
    pub fn put_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn put_i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
}

// ---- thrift compact protocol

struct T {
    out: Vec<u8>,
    last: Vec<i16>,
}

const I32: u8 = 5;
const I64: u8 = 6;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

impl T {
    fn new() -> Self {
        T { out: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - *self.last.last().unwrap();
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, I64);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, BINARY);
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, STRUCT);
        self.last.push(0);
    }
    /// Begin a struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, LIST);
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

/// Definition levels (bit width 1) as a single bit-packed RLE-hybrid run, with
/// the 4-byte length prefix data page v1 uses.
fn def_levels(defined: &[bool]) -> Vec<u8> {
    let groups = defined.len().div_ceil(8);
    let mut body = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        body.push((h as u8) | 0x80);
        h >>= 7;
    }
    body.push(h as u8);
    for g in defined.chunks(8) {
        let mut b = 0u8;
        for (i, &d) in g.iter().enumerate() {
            b |= (d as u8) << i;
        }
        body.push(b);
    }
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Dictionary indices: bit width byte, then one bit-packed RLE-hybrid run.
fn dict_indices(idx: &[u32], ndict: usize) -> Vec<u8> {
    let w = (usize::BITS - ndict.saturating_sub(1).leading_zeros()).max(1) as usize;
    let groups = idx.len().div_ceil(8);
    let mut out = Vec::with_capacity(groups * w + 12);
    out.push(w as u8);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        out.push((h as u8) | 0x80);
        h >>= 7;
    }
    out.push(h as u8);
    let (mut acc, mut nb) = (0u64, 0usize);
    for &v in idx.iter().chain(std::iter::repeat(&0).take(groups * 8 - idx.len())) {
        debug_assert!((v as usize) < ndict.max(1));
        acc |= (v as u64) << nb;
        nb += w;
        while nb >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            nb -= 8;
        }
    }
    debug_assert_eq!(nb, 0);
    out
}

pub fn write(path: &std::path::Path, table: &str, cols: &[Column], rows: usize) -> std::io::Result<()> {
    let mut file = Vec::<u8>::with_capacity(cols.iter().map(|c| c.data.len() + c.defined.len() / 8 + 64).sum::<usize>() + 4096);
    file.extend_from_slice(b"PAR1");
    // (chunk start, total size, dictionary page offset, data page offset) per column
    let mut chunks = Vec::new();
    for c in cols.iter().filter(|_| rows > 0) {
        let start = file.len();
        let mut dict_off = None;
        let body = if let Some(ndict) = c.dict {
            let mut h = T::new();
            h.i32(1, 2); // DICTIONARY_PAGE
            h.i32(2, c.data.len() as i32);
            h.i32(3, c.data.len() as i32);
            h.begin(7);
            h.i32(1, ndict as i32);
            h.i32(2, 0); // PLAIN
            h.end();
            h.out.push(0); // end PageHeader
            dict_off = Some(file.len());
            file.extend_from_slice(&h.out);
            file.extend_from_slice(&c.data);
            dict_indices(&c.idx, ndict)
        } else {
            Vec::new()
        };
        let levels = if c.optional { def_levels(&c.defined) } else { Vec::new() };
        let values: &[u8] = if c.dict.is_some() { &body } else { &c.data };
        let page_len = levels.len() + values.len();
        let mut h = T::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_len as i32);
        h.i32(3, page_len as i32);
        h.begin(5);
        h.i32(1, rows as i32); // num_values (including nulls)
        h.i32(2, if c.dict.is_some() { 8 } else { 0 }); // RLE_DICTIONARY or PLAIN
        h.i32(3, 3); // RLE definition levels
        h.i32(4, 3); // RLE repetition levels
        h.end();
        h.out.push(0); // end PageHeader
        let offset = file.len();
        file.extend_from_slice(&h.out);
        file.extend_from_slice(&levels);
        file.extend_from_slice(values);
        chunks.push((start, file.len() - start, dict_off, offset));
    }
    // FileMetaData
    let mut m = T::new();
    m.i32(1, 1);
    m.list(2, STRUCT, cols.len() + 1);
    m.begin_elem();
    m.bin(4, table.as_bytes());
    m.i32(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin_elem();
        let conv = match c.kind {
            Kind::Str => Some(0),  // UTF8
            Kind::Date => Some(6), // DATE
            Kind::Int32 | Kind::Int64 => None,
        };
        m.i32(1, c.kind.physical());
        m.i32(3, if c.optional { 1 } else { 0 });
        m.bin(4, c.name.as_bytes());
        if let Some(cv) = conv {
            m.i32(6, cv);
        }
        m.end();
    }
    m.i64(3, rows as i64);
    let total: usize = chunks.iter().map(|c| c.1).sum();
    if rows == 0 {
        m.list(4, STRUCT, 0);
        m.bin(6, b"hamsters");
        m.out.push(0);
        file.extend_from_slice(&m.out);
        file.extend_from_slice(&(m.out.len() as u32).to_le_bytes());
        file.extend_from_slice(b"PAR1");
        return save(path, &file);
    }
    m.list(4, STRUCT, 1);
    m.begin_elem();
    m.list(1, STRUCT, cols.len());
    for (c, &(start, size, dict_off, off)) in cols.iter().zip(&chunks) {
        m.begin_elem(); // ColumnChunk
        m.i64(2, start as i64);
        m.begin(3); // ColumnMetaData
        m.i32(1, c.kind.physical());
        // encodings: PLAIN, RLE (levels), RLE_DICTIONARY if a dictionary column
        let mut enc = vec![0];
        if c.optional || c.dict.is_some() {
            enc.push(3);
        }
        if c.dict.is_some() {
            enc.push(8);
        }
        m.list(2, I32, enc.len());
        for e in enc {
            m.zigzag(e);
        }
        m.list(3, BINARY, 1);
        m.varint(c.name.len() as u64);
        m.out.extend_from_slice(c.name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, rows as i64);
        m.i64(6, size as i64);
        m.i64(7, size as i64);
        m.i64(9, off as i64);
        if let Some(d) = dict_off {
            m.i64(11, d as i64);
        }
        m.end();
        m.end();
    }
    m.i64(2, total as i64);
    m.i64(3, rows as i64);
    m.end(); // RowGroup
    m.bin(6, b"hamsters");
    m.out.push(0); // end FileMetaData
    file.extend_from_slice(&m.out);
    file.extend_from_slice(&(m.out.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    save(path, &file)
}

/// Writes the file and starts its writeback at once: dirty output pages left
/// for the end can cost the container time after the process exits.
fn save(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(bytes)?;
    use std::os::fd::AsRawFd;
    unsafe { libc::sync_file_range(f.as_raw_fd(), 0, 0, libc::SYNC_FILE_RANGE_WRITE) };
    Ok(())
}
