use crate::MetricValue;

pub(crate) fn for_each_loki_result_metric(
    value: &mut crate::Value,
    mut apply: impl FnMut(&mut serde_json::Map<String, crate::Value>),
) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(crate::Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(metric) = series
            .get_mut("metric")
            .and_then(crate::Value::as_object_mut)
        else {
            continue;
        };
        apply(metric);
    }
}

pub(crate) struct MetricOperandValues {
    pub(crate) left: MetricValue,
    pub(crate) right: MetricValue,
}

pub(crate) mod binary_arithmetic;
pub(crate) mod binary_sets;
pub(crate) mod expression_parser;
pub(crate) mod expressions;
pub(crate) mod http_queries;
pub(crate) mod result_transforms;
pub(crate) mod scalar_samples;
pub(crate) mod validation;
