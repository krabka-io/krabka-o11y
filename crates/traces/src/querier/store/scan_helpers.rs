use super::*;
use krabka_traceql::matcher_attributes_match;

/// One nanosecond past the newest `max_ts` across `tenant`'s indexed blocks,
/// or 0 when the tenant has none.
///
/// This is what [`crate::querier::live::LiveSource::block_builder_frontier_ns`]
/// reports for a live source backed by the shared trace index.
#[must_use]
pub fn block_builder_frontier_ns(trace_index: &SharedTraceIndex, tenant: &str) -> i64 {
    trace_index
        .load()
        .trace_blocks(tenant)
        .iter()
        .map(|block| block.max_ts.saturating_add(1))
        .max()
        .unwrap_or_default()
}

pub(crate) const INSTRUMENTATION_TAGS: &[&str] =
    &["instrumentation:name", "instrumentation:version"];

pub(crate) fn row_matcher_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    let is_match = match matcher.scope {
        MatchScope::Event => event_values(batch, row)?
            .iter()
            .any(|event| matcher_attributes_match(&event.attributes, matcher)),
        MatchScope::Link => link_values(batch, row)?
            .iter()
            .any(|link| matcher_attributes_match(&link.attributes, matcher)),
        MatchScope::Intrinsic => intrinsic_matches(batch, row, matcher)?,
        MatchScope::Resource => resource_matches(batch, row, matcher)?,
        MatchScope::Instrumentation => instrumentation_matches(batch, row, matcher)?,
        MatchScope::Both => {
            resource_matches(batch, row, matcher)?
                || batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Span => {
            batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Parent => true,
    };
    Ok(is_match != matcher.negated)
}

pub(crate) fn row_matches(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    if !nested_event_matchers_match(batch, row, matchers)?
        || !nested_link_matchers_match(batch, row, matchers)?
    {
        return Ok(false);
    }
    matchers.iter().try_fold(true, |matched, matcher| {
        if !matched {
            return Ok(false);
        }
        if is_event_matcher(matcher) || is_link_matcher(matcher) {
            return Ok(true);
        }
        row_matcher_matches(batch, row, matcher)
    })
}

pub(crate) const SCOPE_ORDER: &[TagScope] = &[
    TagScope::Resource,
    TagScope::Span,
    TagScope::Intrinsic,
    TagScope::Event,
    TagScope::Link,
    TagScope::Instrumentation,
];

/// A `TraceIndex` shared between the span store and the live sources.
///
/// It is swappable at runtime, so a background task can reload it without a
/// restart.
pub type SharedTraceIndex = Arc<ArcSwap<TraceIndex>>;

pub(crate) fn trace_from_batches(
    trace_id: &[u8; 16],
    batches: Vec<RecordBatch>,
) -> Result<Option<TraceSpans>, TraceqlError> {
    let mut root_service_name = String::new();
    let mut root_trace_name = String::new();
    let mut resource_attributes = Vec::new();
    let mut spans = Vec::new();

    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        for row in 0..batch.num_rows() {
            if trace_ids.value(row) != trace_id {
                continue;
            }
            if root_service_name.is_empty() {
                root_service_name = string_value(&batch, COL_ROOT_SERVICE_NAME, row)?;
            }
            if root_trace_name.is_empty() {
                root_trace_name = string_value(&batch, COL_ROOT_SPAN_NAME, row)?;
            }
            if resource_attributes.is_empty() {
                resource_attributes = resource_attr_values(&batch, row)?;
            }
            spans.push(SpanRef {
                span_id: fixed_value::<8>(&batch, COL_SPAN_ID, row)?,
                parent_span_id: nullable_fixed_value::<8>(&batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(&batch, COL_NAME, row)?,
                kind: int32_value(&batch, COL_KIND, row)?,
                nested_set_left: int32_value(&batch, COL_NS_LEFT, row)?,
                nested_set_right: int32_value(&batch, COL_NS_RIGHT, row)?,
                nested_set_parent: int32_value(&batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64::try_from(int64_value(&batch, COL_START, row)?)
                    .unwrap_or(0),
                duration: Time::from_nanos(int64_value(&batch, COL_DURATION, row)?),
                status_code: int32_value(&batch, COL_STATUS_CODE, row)?,
                status_message: string_value(&batch, COL_STATUS_MESSAGE, row)?,
                instrumentation_name: string_value(&batch, COL_INSTRUMENTATION_NAME, row)?,
                instrumentation_version: string_value(&batch, COL_INSTRUMENTATION_VERSION, row)?,
                resource_attributes: resource_attr_values(&batch, row)?,
                attributes: attr_values(&batch, row)?,
                events: event_values(&batch, row)?,
                links: link_values(&batch, row)?,
            });
        }
    }

    deduplicate_trace_spans(&mut spans);
    Ok((!spans.is_empty()).then_some(TraceSpans {
        trace_id: *trace_id,
        root_service_name,
        root_trace_name,
        resource_attributes,
        spans,
    }))
}


pub(crate) fn intrinsic_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "span:name" => string_matches(
            &string_value(batch, COL_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "event:name" => {
            let events = event_values(batch, row)?;
            nested_presence_matches(!events.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    events
                        .iter()
                        .any(|event| string_matches(&event.name, matcher.op, &matcher.value))
                },
            )
        }
        "event:timeSinceStart" => {
            let events = event_values(batch, row)?;
            nested_presence_matches(!events.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    events.iter().any(|event| {
                        int_matches(
                            event.time_since_start.nanos_i64(),
                            matcher.op,
                            &matcher.value,
                        )
                    })
                },
            )
        }
        "link:traceID" => link_matches(batch, row, matcher, |link| bytes_to_hex(&link.trace_id))?,
        "link:spanID" => link_matches(batch, row, matcher, |link| bytes_to_hex(&link.span_id))?,
        "trace:id" => string_matches(
            &bytes_to_hex(&fixed_value::<16>(batch, COL_TRACE_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "trace:rootService" => string_matches(
            &string_value(batch, COL_ROOT_SERVICE_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "trace:rootName" => string_matches(
            &string_value(batch, COL_ROOT_SPAN_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "trace:duration" => int_matches(
            int64_value(batch, COL_TRACE_DURATION, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:duration" => int_matches(
            int64_value(batch, COL_DURATION, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:id" => string_matches(
            &bytes_to_hex(&fixed_value::<8>(batch, COL_SPAN_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:parentID" => nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)?.map_or_else(
            || nil_matches(matcher.op, &matcher.value),
            |parent| string_matches(&bytes_to_hex(&parent), matcher.op, &matcher.value),
        ),
        "span:kind" => enum_int_matches(
            i64::from(int32_value(batch, COL_KIND, row)?),
            matcher.op,
            &matcher.value,
            kind_enum_value,
        ),
        "span:status" => enum_int_matches(
            i64::from(int32_value(batch, COL_STATUS_CODE, row)?),
            matcher.op,
            &matcher.value,
            status_enum_value,
        ),
        "span:statusMessage" => string_matches(
            &string_value(batch, COL_STATUS_MESSAGE, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:childCount" => int_matches(
            i64::from(int32_value(batch, COL_CHILD_COUNT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetLeft" => int_matches(
            i64::from(int32_value(batch, COL_NS_LEFT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetRight" => int_matches(
            i64::from(int32_value(batch, COL_NS_RIGHT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetParent" | "span:Parent" => int_matches(
            i64::from(int32_value(batch, COL_PARENT_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "instrumentation:name" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "instrumentation:version" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_VERSION, row)?,
            matcher.op,
            &matcher.value,
        ),
        _ => true,
    })
}

fn link_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
    value: impl Fn(&LinkRef) -> String,
) -> Result<bool, TraceqlError> {
    let links = link_values(batch, row)?;
    Ok(
        nested_presence_matches(!links.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
            || {
                links
                    .iter()
                    .any(|link| string_matches(&value(link), matcher.op, &matcher.value))
            },
        ),
    )
}
