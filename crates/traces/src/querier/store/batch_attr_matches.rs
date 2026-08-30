use super::{MatchCmp, MatchValue, RecordBatch, TraceqlError, batch_attr_matches_with_resource};

pub(crate) fn batch_attr_matches(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> Result<bool, TraceqlError> {
    batch_attr_matches_with_resource(batch, row, key, op, expected, false)
}
