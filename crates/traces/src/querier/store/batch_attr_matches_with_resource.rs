use super::{RecordBatch, MatchCmp, MatchValue, TraceqlError, attr_values_with_resource, attr_values_match};

pub(crate) fn batch_attr_matches_with_resource(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
    include_resource: bool,
) -> Result<bool, TraceqlError> {
    let attrs = attr_values_with_resource(batch, row, include_resource)?;
    let values = attrs
        .iter()
        .filter(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    Ok(attr_values_match(&values, op, expected))
}
