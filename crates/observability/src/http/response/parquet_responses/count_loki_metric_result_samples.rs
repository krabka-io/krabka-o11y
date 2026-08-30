use super::Value;

pub(crate) fn count_loki_metric_result_samples(value: &Value) -> u64 {
    let Some(results) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    results
        .iter()
        .map(|result| {
            if let Some(values) = result.get("values").and_then(Value::as_array) {
                u64::try_from(values.len()).unwrap_or(u64::MAX)
            } else {
                u64::from(result.get("value").is_some())
            }
        })
        .fold(0_u64, u64::saturating_add)
}
