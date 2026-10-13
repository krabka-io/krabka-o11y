use super::{BTreeSet, Span, bytes_to_hex};

pub(crate) fn collect_span_intrinsic_value(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    match tag {
        "span:duration" => {
            values.insert(krabka_traceql::TypedValue::new(
                "duration",
                span.duration_ns.to_string(),
            ));
        }
        "span:id" => {
            values.insert(krabka_traceql::TypedValue::new(
                "string",
                bytes_to_hex(&span.span_id),
            ));
        }
        "span:kind" => {
            values.insert(krabka_traceql::TypedValue::new(
                "int",
                span.kind.as_i32().to_string(),
            ));
        }
        "span:name" => {
            values.insert(krabka_traceql::TypedValue::new("string", span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    bytes_to_hex(&parent_id),
                ));
            }
        }
        "span:status" => {
            values.insert(krabka_traceql::TypedValue::new(
                "int",
                span.status.as_i32().to_string(),
            ));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    span.status_message.clone(),
                ));
            }
        }
        "trace:id" => {
            values.insert(krabka_traceql::TypedValue::new(
                "string",
                bytes_to_hex(&span.trace_id),
            ));
        }
        _ => {}
    }
}
