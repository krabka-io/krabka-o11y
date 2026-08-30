use super::*;

pub(crate) fn sort_loki_vector_result(value: &mut Value, descending: bool) {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector") {
        return;
    }
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    results.sort_by(|left, right| {
        let ordering = match (
            loki_vector_sample_value(left),
            loki_vector_sample_value(right),
        ) {
            (Some(left), Some(right)) => left.cmp_value(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
}
