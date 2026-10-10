use super::{
    AttrMatch, MatchCmp, MatchValue, RecordBatch, ResourceAttrs, TraceqlError,
    batch_attr_matches_with_resource,
};

pub(crate) fn batch_attr_matches(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> Result<bool, TraceqlError> {
    batch_attr_matches_with_resource(
        batch,
        row,
        AttrMatch {
            key,
            op,
            expected,
            resource: ResourceAttrs::Exclude,
        },
    )
}
