use super::push_uvarint;

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
