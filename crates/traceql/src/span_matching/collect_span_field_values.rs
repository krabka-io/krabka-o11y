use super::{BTreeSet, SpanIntrinsicFields, TimeExt as _, bytes_to_hex};

/// Adds the `(type, value)` pair that `tag` lists for one span, when `tag`
/// is a span or instrumentation intrinsic read off the span alone.
///
/// The nested-set intrinsics (`span:childCount`, `span:nestedSet*`) need the
/// rest of the trace, so the caller resolves those itself.
pub fn collect_span_field_values(
    span: SpanIntrinsicFields<'_>,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "span:duration" => {
            values.insert((
                "duration".to_string(),
                span.duration.nanos_i64().to_string(),
            ));
        }
        "span:id" => {
            values.insert(("string".to_string(), bytes_to_hex(&span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".to_string(), span.kind.to_string()));
        }
        "span:name" => {
            values.insert(("string".to_string(), span.name.to_string()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert(("int".to_string(), span.status_code.to_string()));
        }
        "span:statusMessage" if !span.status_message.is_empty() => {
            values.insert(("string".to_string(), span.status_message.to_string()));
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_name.to_string()));
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert((
                "string".to_string(),
                span.instrumentation_version.to_string(),
            ));
        }
        _ => {}
    }
}
