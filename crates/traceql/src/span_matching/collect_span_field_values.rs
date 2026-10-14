use super::{BTreeSet, SpanIntrinsicFields, TimeExt as _, TypedValue, bytes_to_hex};

/// Adds the `(type, value)` pair that `tag` lists for one span, when `tag`
/// is a span or instrumentation intrinsic read off the span alone.
///
/// The nested-set intrinsics (`span:childCount`, `span:nestedSet*`) need the
/// rest of the trace, so the caller resolves those itself.
pub fn collect_span_field_values(
    span: SpanIntrinsicFields<'_>,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "span:duration" => {
            values.insert(TypedValue {
                type_: "duration".to_string(),
                value: span.duration.nanos_i64().to_string(),
            });
        }
        "span:id" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: bytes_to_hex(&span.span_id),
            });
        }
        "span:kind" => {
            values.insert(TypedValue {
                type_: "int".to_string(),
                value: span.kind.to_string(),
            });
        }
        "span:name" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.name.to_string(),
            });
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: bytes_to_hex(&parent_id),
                });
            }
        }
        "span:status" => {
            values.insert(TypedValue {
                type_: "int".to_string(),
                value: span.status_code.to_string(),
            });
        }
        "span:statusMessage" if !span.status_message.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.status_message.to_string(),
            });
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.instrumentation_name.to_string(),
            });
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.instrumentation_version.to_string(),
            });
        }
        _ => {}
    }
}
