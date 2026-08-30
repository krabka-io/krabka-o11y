use super::*;

pub(crate) fn loki_metric_sample_timestamp_key(sample: &Value) -> Option<String> {
    sample
        .as_array()
        .and_then(|sample| sample.first())
        .map(Value::to_string)
}
