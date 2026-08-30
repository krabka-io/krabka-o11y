use super::*;

pub(crate) fn intrinsic_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    match matcher.key.as_str() {
        "name" | "span:name" => string_matches(&span.name, matcher.op, &matcher.value),
        "event:name" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events
                        .iter()
                        .any(|event| string_matches(&event.name, matcher.op, &matcher.value))
                })
        }
        "event:timeSinceStart" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events.iter().any(|event| {
                        int_matches(
                            event.time_since_start.nanos_i64(),
                            matcher.op,
                            &matcher.value,
                        )
                    })
                })
        }
        "link:traceID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                    })
                })
        }
        "link:spanID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                    })
                })
        }
        "trace:id" => string_matches(&bytes_to_hex(&trace.trace_id), matcher.op, &matcher.value),
        "trace:rootService" => string_matches(&trace.root_service_name, matcher.op, &matcher.value),
        "trace:rootName" => string_matches(&trace.root_span_name, matcher.op, &matcher.value),
        "trace:duration" => {
            int_matches(trace.trace_duration.nanos_i64(), matcher.op, &matcher.value)
        }
        "duration" | "span:duration" => {
            int_matches(span.duration.nanos_i64(), matcher.op, &matcher.value)
        }
        "span:id" => string_matches(&bytes_to_hex(&span.span_id), matcher.op, &matcher.value),
        "span:parentID" => span.parent_span_id.map_or_else(
            || nil_matches(matcher.op, &matcher.value),
            |parent| string_matches(&bytes_to_hex(&parent), matcher.op, &matcher.value),
        ),
        "kind" | "span:kind" => enum_int_matches(
            i64::from(span.kind),
            matcher.op,
            &matcher.value,
            kind_enum_value,
        ),
        "status" | "span:status" => enum_int_matches(
            i64::from(span.status_code),
            matcher.op,
            &matcher.value,
            status_enum_value,
        ),
        "statusMessage" | "span:statusMessage" => {
            string_matches(&span.status_message, matcher.op, &matcher.value)
        }
        "span:childCount" => int_matches(
            i64::from(child_count_for(nested_sets, idx)),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetLeft" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.left), matcher.op, &matcher.value)),
        "span:nestedSetRight" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.right), matcher.op, &matcher.value)),
        "span:nestedSetParent" => nested_sets.get(idx).is_some_and(|nested| {
            int_matches(i64::from(nested.parent_id), matcher.op, &matcher.value)
        }),
        "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => true,
    }
}
