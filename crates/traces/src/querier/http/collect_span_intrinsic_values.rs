use krabka_traceql::collect_span_field_values;

use super::{BTreeSet, SpanRef};

pub(crate) fn collect_span_intrinsic_values(
    span: &SpanRef,
    spans: &[SpanRef],
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "span:childCount" => {
            let count = spans
                .iter()
                .filter(|other| other.nested_set_parent == span.nested_set_left)
                .count();
            values.insert(("int".to_string(), count.to_string()));
        }
        "span:nestedSetLeft" => {
            values.insert(("int".to_string(), span.nested_set_left.to_string()));
        }
        "span:nestedSetParent" | "span:Parent" => {
            values.insert(("int".to_string(), span.nested_set_parent.to_string()));
        }
        "span:nestedSetRight" => {
            values.insert(("int".to_string(), span.nested_set_right.to_string()));
        }
        _ => collect_span_field_values(span.into(), tag, values),
    }
}
