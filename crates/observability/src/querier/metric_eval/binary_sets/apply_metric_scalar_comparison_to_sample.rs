use super::{
    MetricScalarComparison, MetricValue, Value, json, metric_scalar_comparison_matches,
    parse_metric_sample_value,
};

pub(crate) fn apply_metric_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(
        sample_value,
        comparison.op,
        scalar,
        comparison.scalar_on_left,
    );
    if comparison.bool_modifier {
        if let Some(value) = values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        matches
    }
}
