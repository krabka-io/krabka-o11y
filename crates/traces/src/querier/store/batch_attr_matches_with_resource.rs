use super::{AttrMatch, RecordBatch, TraceqlError, attr_values_match, attr_values_with_resource};

pub(crate) fn batch_attr_matches_with_resource(
    batch: &RecordBatch,
    row: usize,
    attr_match: AttrMatch<'_>,
) -> Result<bool, TraceqlError> {
    let AttrMatch {
        key,
        op,
        expected,
        resource,
    } = attr_match;
    let attrs = attr_values_with_resource(batch, row, resource)?;
    let values = attrs
        .iter()
        .filter(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    Ok(attr_values_match(&values, op, expected))
}
