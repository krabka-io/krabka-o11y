use super::push_uvarint;

/// Appends a length or a count to `out` as an unsigned varint.
pub(crate) fn push_len(out: &mut Vec<u8>, len: usize) {
    push_uvarint(
        out,
        u64::try_from(len).expect("a length in memory fits a u64"),
    );
}
