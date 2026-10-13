use super::{Ordering, SampleOrder, Value, loki_vector_sample_value};

pub(crate) fn sort_loki_vector_result(value: &mut Value, order: SampleOrder) {
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
        let left_value = loki_vector_sample_value(left);
        let right_value = loki_vector_sample_value(right);
        let ordering = if let Some(left_value) = left_value {
            if let Some(right_value) = right_value {
                left_value.cmp_value(right_value)
            } else {
                Ordering::Less
            }
        } else if right_value.is_some() {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
        order.orient(ordering)
    });
}
