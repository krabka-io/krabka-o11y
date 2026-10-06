//! A minimal Prometheus TSDB block writer for the import tests.
//!
//! It writes the index, chunk segment and tombstones layouts that the
//! Prometheus writer uses, with XOR chunks only, and it writes exactly what
//! the test asks for: labels out of order, duplicate series or out-of-order
//! samples reach the file unchanged. That is how the tests build blocks that
//! the real writer refuses to produce.

use std::collections::BTreeMap;

pub struct SyntheticSeries {
    pub labels: Vec<(Vec<u8>, Vec<u8>)>,
    pub chunks: Vec<SyntheticChunk>,
}

pub struct SyntheticChunk {
    /// The chunk segment that holds the chunk.
    pub segment: usize,
    pub samples: Vec<(i64, f64)>,
}

pub struct SyntheticBlock {
    pub index: Vec<u8>,
    pub segments: Vec<Vec<u8>>,
}

/// Writes `series` in the given order into an index and `segments` chunk
/// segment files.
#[must_use]
pub fn write_block(series: &[SyntheticSeries], segments: usize) -> SyntheticBlock {
    let mut files = vec![segment_header(); segments];
    let mut chunk_refs = Vec::new();
    for entry in series {
        let mut refs = Vec::new();
        for chunk in &entry.chunks {
            let file = &mut files[chunk.segment];
            let offset = u64::try_from(file.len()).expect("segment offset fits u64");
            let data = xor_chunk(&chunk.samples);
            put_uvarint(
                file,
                u64::try_from(data.len()).expect("chunk length fits u64"),
            );
            let mut covered = vec![1_u8];
            covered.extend_from_slice(&data);
            file.extend_from_slice(&covered);
            file.extend_from_slice(&crc32c::crc32c(&covered).to_be_bytes());
            let segment = u64::try_from(chunk.segment).expect("segment fits u64");
            refs.push((segment << 32) | offset);
        }
        chunk_refs.push(refs);
    }
    SyntheticBlock {
        index: write_index(series, &chunk_refs),
        segments: files,
    }
}

/// Writes a tombstones file that deletes each `(series reference, min, max)`
/// interval, inclusive at both ends.
#[must_use]
pub fn write_tombstones(records: &[(u64, i64, i64)]) -> Vec<u8> {
    let mut file = 0x0130_BA30_u32.to_be_bytes().to_vec();
    file.push(1);
    let mut body = Vec::new();
    for (series, min_time, max_time) in records {
        put_uvarint(&mut body, *series);
        put_varint(&mut body, *min_time);
        put_varint(&mut body, *max_time);
    }
    file.extend_from_slice(&body);
    file.extend_from_slice(&crc32c::crc32c(&body).to_be_bytes());
    file
}

fn segment_header() -> Vec<u8> {
    let mut header = 0x85BD_40DD_u32.to_be_bytes().to_vec();
    header.extend_from_slice(&[1, 0, 0, 0]);
    header
}

fn write_index(series: &[SyntheticSeries], chunk_refs: &[Vec<u64>]) -> Vec<u8> {
    let mut symbols = series
        .iter()
        .flat_map(|entry| {
            entry
                .labels
                .iter()
                .flat_map(|(name, value)| [name.as_slice(), value.as_slice()])
        })
        .collect::<Vec<_>>();
    symbols.sort_unstable();
    symbols.dedup();
    let symbol_ref = |symbol: &[u8]| {
        u64::try_from(
            symbols
                .binary_search(&symbol)
                .expect("symbol is in the table"),
        )
        .expect("symbol reference fits u64")
    };

    let mut index = 0xBAAA_D700_u32.to_be_bytes().to_vec();
    index.push(2);
    let symbols_offset = offset(&index);
    let mut content = u32::try_from(symbols.len())
        .expect("symbol count")
        .to_be_bytes()
        .to_vec();
    for symbol in &symbols {
        put_uvarint(
            &mut content,
            u64::try_from(symbol.len()).expect("symbol length"),
        );
        content.extend_from_slice(symbol);
    }
    put_section(&mut index, &content);

    align(&mut index, 16);
    let series_offset = offset(&index);
    let mut postings = BTreeMap::<(&[u8], &[u8]), Vec<u32>>::new();
    for (entry, refs) in series.iter().zip(chunk_refs) {
        align(&mut index, 16);
        let reference = u32::try_from(index.len() / 16).expect("series reference fits u32");
        postings.entry((b"", b"")).or_default().push(reference);
        let mut content = Vec::new();
        put_uvarint(
            &mut content,
            u64::try_from(entry.labels.len()).expect("label count"),
        );
        for (name, value) in &entry.labels {
            postings
                .entry((name.as_slice(), value.as_slice()))
                .or_default()
                .push(reference);
            put_uvarint(&mut content, symbol_ref(name.as_slice()));
            put_uvarint(&mut content, symbol_ref(value.as_slice()));
        }
        put_uvarint(
            &mut content,
            u64::try_from(entry.chunks.len()).expect("chunk count"),
        );
        let mut previous = None::<(i64, u64)>;
        for (chunk, reference) in entry.chunks.iter().zip(refs) {
            let min_time = chunk
                .samples
                .iter()
                .map(|(t, _)| *t)
                .min()
                .expect("chunk has samples");
            let max_time = chunk
                .samples
                .iter()
                .map(|(t, _)| *t)
                .max()
                .expect("chunk has samples");
            match previous {
                None => {
                    put_varint(&mut content, min_time);
                    put_uvarint(&mut content, (max_time - min_time).cast_unsigned());
                    put_uvarint(&mut content, *reference);
                }
                Some((previous_max, previous_ref)) => {
                    put_uvarint(&mut content, (min_time - previous_max).cast_unsigned());
                    put_uvarint(&mut content, (max_time - min_time).cast_unsigned());
                    put_varint(
                        &mut content,
                        reference.cast_signed() - previous_ref.cast_signed(),
                    );
                }
            }
            previous = Some((max_time, *reference));
        }
        put_uvarint(
            &mut index,
            u64::try_from(content.len()).expect("entry length"),
        );
        index.extend_from_slice(&content);
        index.extend_from_slice(&crc32c::crc32c(&content).to_be_bytes());
    }

    align(&mut index, 4);
    let postings_offset = offset(&index);
    let mut table = u32::try_from(postings.len())
        .expect("pair count")
        .to_be_bytes()
        .to_vec();
    for ((name, value), refs) in &postings {
        align(&mut index, 4);
        put_uvarint(&mut table, 2);
        for part in [name, value] {
            put_uvarint(&mut table, u64::try_from(part.len()).expect("label length"));
            table.extend_from_slice(part);
        }
        put_uvarint(&mut table, offset(&index));
        let mut list = u32::try_from(refs.len())
            .expect("list length")
            .to_be_bytes()
            .to_vec();
        for reference in refs {
            list.extend_from_slice(&reference.to_be_bytes());
        }
        put_section(&mut index, &list);
    }
    let table_offset = offset(&index);
    put_section(&mut index, &table);

    let mut toc = Vec::new();
    for value in [
        symbols_offset,
        series_offset,
        postings_offset,
        table_offset,
        postings_offset,
        table_offset,
    ] {
        toc.extend_from_slice(&value.to_be_bytes());
    }
    index.extend_from_slice(&toc);
    index.extend_from_slice(&crc32c::crc32c(&toc).to_be_bytes());
    index
}

fn offset(file: &[u8]) -> u64 {
    u64::try_from(file.len()).expect("offset fits u64")
}

fn align(file: &mut Vec<u8>, to: usize) {
    file.resize(file.len().next_multiple_of(to), 0);
}

fn put_section(file: &mut Vec<u8>, content: &[u8]) {
    file.extend_from_slice(
        &u32::try_from(content.len())
            .expect("section length")
            .to_be_bytes(),
    );
    file.extend_from_slice(content);
    file.extend_from_slice(&crc32c::crc32c(content).to_be_bytes());
}

fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).expect("seven bits") | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).expect("seven bits"));
}

fn put_varint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, ((value << 1) ^ (value >> 63)).cast_unsigned());
}

/// Encodes an XOR chunk. Every delta-of-delta uses the 64-bit form and every
/// changed value writes new leading and trailing counts, which the format
/// allows and which keeps the writer short.
fn xor_chunk(samples: &[(i64, f64)]) -> Vec<u8> {
    let mut bits = BitWriter::default();
    let mut previous_time = 0_i64;
    let mut previous_delta = 0_i64;
    let mut previous_value = 0_u64;
    for (index, (timestamp, value)) in samples.iter().enumerate() {
        let value = value.to_bits();
        if index == 0 {
            let mut varint = Vec::new();
            put_varint(&mut varint, *timestamp);
            bits.write_bytes(&varint);
            bits.write(value, 64);
        } else {
            let delta = timestamp - previous_time;
            if index == 1 {
                let mut uvarint = Vec::new();
                put_uvarint(&mut uvarint, delta.cast_unsigned());
                bits.write_bytes(&uvarint);
            } else {
                bits.write(0b1111, 4);
                bits.write((delta - previous_delta).cast_unsigned(), 64);
            }
            previous_delta = delta;
            let xor = value ^ previous_value;
            if xor == 0 {
                bits.write(0, 1);
            } else {
                bits.write(0b11, 2);
                bits.write(0, 5);
                bits.write(0, 6);
                bits.write(xor, 64);
            }
        }
        previous_time = *timestamp;
        previous_value = value;
    }
    let mut chunk = u16::try_from(samples.len())
        .expect("sample count fits u16")
        .to_be_bytes()
        .to_vec();
    chunk.extend_from_slice(&bits.bytes);
    chunk
}

#[derive(Default)]
struct BitWriter {
    bytes: Vec<u8>,
    used: u8,
}

impl BitWriter {
    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.write(u64::from(*byte), 8);
        }
    }

    fn write(&mut self, value: u64, count: u8) {
        for shift in (0..count).rev() {
            if self.used == 0 {
                self.bytes.push(0);
            }
            if (value >> shift) & 1 == 1 {
                let last = self.bytes.last_mut().expect("a byte was pushed");
                *last |= 0x80 >> self.used;
            }
            self.used = (self.used + 1) % 8;
        }
    }
}
