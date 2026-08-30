use super::{
    ATTR_PREFIX, INSTRUMENTATION_ATTR_PREFIX, MatchScope, RESOURCE_ATTR_PREFIX, RecordBatch,
    SpanMatcher, TraceqlError, add_span_attr_columns_to_batch,
};

/// Materialize the regular span and resource attribute columns, `attr.<key>`,
/// that metric `by()` and `select()` projections reference.
///
/// The selector path filters attributes directly on the parquet arrays, so
/// nothing else builds these columns, and `rate() by(span.http.method)` cannot
/// `GROUP BY attr.http.method` without them.
///
/// This converts values into a Utf8 column, because metric labels are strings.
/// A span that lacks the attribute becomes NULL, which is the nil group and
/// matches Tempo.
///
/// `add_nested_intrinsic_columns` handles the Event and Link matchers. This
/// function skips `service.name`, which is the promoted
/// `COL_ROOT_SERVICE_NAME` column rather than an attribute.
pub(crate) fn add_span_attr_columns(
    batches: Vec<RecordBatch>,
    projection_matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    // (column_name, attr-array lookup key, include_resource) per regular-attr field.
    let mut wanted: Vec<(String, String, bool)> = Vec::new();
    for matcher in projection_matchers {
        let (lookup_key, include_resource) = match matcher.scope {
            MatchScope::Span | MatchScope::Both => (matcher.key.clone(), false),
            MatchScope::Resource => (format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key), true),
            MatchScope::Instrumentation => (
                format!("{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key),
                false,
            ),
            _ => continue,
        };
        if matcher.key == "service.name" {
            continue; // grouped via the promoted COL_ROOT_SERVICE_NAME column
        }
        let column_name = if matcher.scope == MatchScope::Instrumentation {
            format!("{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key)
        } else {
            format!("{ATTR_PREFIX}{}", matcher.key)
        };
        if !wanted.iter().any(|(name, _, _)| name == &column_name) {
            wanted.push((column_name, lookup_key, include_resource));
        }
    }
    if wanted.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| add_span_attr_columns_to_batch(&batch, &wanted))
        .collect()
}
