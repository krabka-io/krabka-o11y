use std::collections::{BTreeMap, BTreeSet};

use super::{BlockEntry, BlockLevel, Labels, SeriesFingerprint};

/// Leading bytes of every index shard object, used to reject unrelated data.
pub(crate) const INDEX_SHARD_MAGIC: [u8; 4] = *b"KBIX";

/// Version of the shard encoding this build writes and accepts.
pub(crate) const INDEX_SHARD_FORMAT_VERSION: u8 = 2;

/// One shard's worth of a tenant's index, ready to encode.
///
/// The postings use ordinals local to this shard, independent of live-index
/// ordinals.
pub(crate) struct IndexShardPayload<'index> {
    pub(crate) tenant: &'index str,
    pub(crate) series: &'index BTreeMap<SeriesFingerprint, Labels>,
    pub(crate) selected: BTreeSet<SeriesFingerprint>,
    pub(crate) blocks: Vec<&'index BlockEntry>,
    pub(crate) postings: BTreeMap<SeriesFingerprint, Vec<u32>>,
}

/// Appends `value` to `out` as an LEB128 unsigned varint.
///
/// Every count, length and ordinal in the index encoding goes through this.
/// The values are almost always small. A block ordinal, a label count and a
/// dictionary id all fit in a byte or two, and a fixed-width field would spend
/// eight bytes on each of them. Seven bits per byte keeps the common ones to
/// one byte.
pub(crate) fn push_uvarint(out: &mut Vec<u8>, value: u64) {
    let mut rest = value;
    while rest >= 0x80 {
        let byte = u8::try_from(rest & 0x7f).expect("seven bits fit a u8");
        out.push(byte | 0x80);
        rest >>= 7;
    }
    out.push(u8::try_from(rest).expect("a value below 0x80 fits a u8"));
}

/// Appends `value` to `out` as a zigzag-encoded varint.
///
/// Timestamps are stored as deltas, and a delta is signed. Zigzag maps a small
/// negative delta onto a small unsigned one, so an out-of-order block costs a
/// byte or two rather than the ten bytes a two's-complement varint spends on
/// every negative number.
pub(crate) fn push_ivarint(out: &mut Vec<u8>, value: i64) {
    let zigzag = (value << 1) ^ (value >> 63);
    push_uvarint(out, u64::from_le_bytes(zigzag.to_le_bytes()));
}

/// Appends a length or a count to `out` as an unsigned varint.
pub(crate) fn push_len(out: &mut Vec<u8>, len: usize) {
    push_uvarint(
        out,
        u64::try_from(len).expect("a length in memory fits a u64"),
    );
}

/// Appends `text` to `out` as its length followed by its bytes.
pub(crate) fn push_string(out: &mut Vec<u8>, text: &str) {
    push_len(out, text.len());
    out.extend_from_slice(text.as_bytes());
}

/// Appends the id `dictionary` gives `text`.
///
/// A shard encoder puts every string it writes by id into its dictionary
/// first, so a miss is a bug in the encoder rather than in the input.
pub(crate) fn push_dictionary_id(
    out: &mut Vec<u8>,
    dictionary: &BTreeMap<String, usize>,
    text: &str,
) {
    let id = dictionary
        .get(text)
        .copied()
        .expect("every dictionary string was put in the dictionary");
    push_len(out, id);
}

pub(crate) fn push_shard_block_prefix(
    out: &mut Vec<u8>,
    object_key: &str,
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    level: BlockLevel,
    previous_min: &mut i64,
) {
    push_string(out, object_key);
    push_ivarint(out, min_ts.wrapping_sub(*previous_min));
    push_ivarint(out, max_ts.wrapping_sub(min_ts));
    *previous_min = min_ts;
    push_len(out, row_count);
    push_uvarint(out, u64::from(level.get()));
}
