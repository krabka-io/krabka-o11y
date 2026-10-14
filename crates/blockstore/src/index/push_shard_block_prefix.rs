use super::{BlockLevel, push_ivarint, push_len, push_string, push_uvarint};

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
