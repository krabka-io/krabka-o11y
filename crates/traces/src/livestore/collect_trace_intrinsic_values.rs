use super::{BTreeSet, Span, attr_string, root_span};

pub(crate) fn collect_trace_intrinsic_values(
    spans: &[&Span],
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    if spans.is_empty() {
        return;
    }
    match tag {
        "trace:duration" => {
            let bounds = crate::trace_time_bounds(spans.iter().map(|span| crate::SpanTiming {
                start_ns: span.start_ns,
                duration_ns: span.duration_ns,
            }));
            values.insert(krabka_traceql::TypedValue::new(
                "duration",
                bounds.end_ns.saturating_sub(bounds.start_ns).to_string(),
            ));
        }
        "trace:rootName" => {
            if let Some(root) = root_span(spans) {
                values.insert(krabka_traceql::TypedValue::new("string", root.name.clone()));
            }
        }
        "trace:rootService" => {
            if let Some(root) = root_span(spans)
                && let Some(service) = attr_string(&root.resource_attrs, "service.name")
            {
                values.insert(krabka_traceql::TypedValue::new("string", service));
            }
        }
        _ => {}
    }
}
