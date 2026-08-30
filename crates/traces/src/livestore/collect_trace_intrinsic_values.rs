use super::{BTreeSet, Span, attr_string, root_span};

pub(crate) fn collect_trace_intrinsic_values(
    spans: &[&Span],
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    if spans.is_empty() {
        return;
    }
    match tag {
        "trace:duration" => {
            let start = spans.iter().map(|span| span.start_ns).min().unwrap_or(0);
            let end = spans
                .iter()
                .map(|span| span.start_ns.saturating_add(span.duration_ns))
                .max()
                .unwrap_or(start);
            values.insert(("duration".into(), end.saturating_sub(start).to_string()));
        }
        "trace:rootName" => {
            if let Some(root) = root_span(spans) {
                values.insert(("string".into(), root.name.clone()));
            }
        }
        "trace:rootService" => {
            if let Some(root) = root_span(spans)
                && let Some(service) = attr_string(&root.resource_attrs, "service.name")
            {
                values.insert(("string".into(), service));
            }
        }
        _ => {}
    }
}
