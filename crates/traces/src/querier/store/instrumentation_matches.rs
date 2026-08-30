use super::{
    COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, INSTRUMENTATION_ATTR_PREFIX,
    RecordBatch, SpanMatcher, TraceqlError, batch_attr_matches, string_matches, string_value,
};

pub(crate) fn instrumentation_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "name" | "instrumentation:name" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "version" | "instrumentation:version" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_VERSION, row)?,
            matcher.op,
            &matcher.value,
        ),
        _ => batch_attr_matches(
            batch,
            row,
            &format!("{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key),
            matcher.op,
            &matcher.value,
        )?,
    })
}
