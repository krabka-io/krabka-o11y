use super::{MetricValue, Value, parse_metric_sample_value};

pub(crate) fn loki_vector_sample_value(sample: &Value) -> Option<MetricValue> {
    sample
        .pointer("/value/1")
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
}
