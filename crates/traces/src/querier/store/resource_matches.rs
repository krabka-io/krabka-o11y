use super::{RecordBatch, SpanMatcher, TraceqlError, root_service_matches, string_value, COL_ROOT_SERVICE_NAME, batch_attr_matches_with_resource, RESOURCE_ATTR_PREFIX};

pub(crate) fn resource_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "service.name" => {
            root_service_matches(&string_value(batch, COL_ROOT_SERVICE_NAME, row)?, matcher)
        }
        _ => batch_attr_matches_with_resource(
            batch,
            row,
            &format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key),
            matcher.op,
            &matcher.value,
            true,
        )?,
    })
}
