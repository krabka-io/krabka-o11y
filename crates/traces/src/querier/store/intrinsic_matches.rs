use super::{Array, COL_CHILD_COUNT, COL_DURATION, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, RecordBatch, SpanMatcher, TimeExt, TraceqlError, bytes_to_hex, enum_int_matches, event_values, fixed_value, int32_value, int64_value, int_matches, kind_enum_value, link_values, nested_presence_matches, nil_matches, nullable_fixed_value, status_enum_value, string_matches, string_value};

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
        "link:traceID" => {
            let links = link_values(batch, row)?;
            nested_presence_matches(!links.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                    })
                },
            )
        }
        "link:spanID" => {
            let links = link_values(batch, row)?;
            nested_presence_matches(!links.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                    })
                },
            )
        }
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
