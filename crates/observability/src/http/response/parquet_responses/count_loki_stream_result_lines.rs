use super::Value;

pub(crate) fn count_loki_stream_result_lines(value: &Value) -> u64 {
    value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .map_or(0, |streams| {
            streams
                .iter()
                .filter_map(|stream| stream.get("values").and_then(Value::as_array))
                .map(|values| u64::try_from(values.len()).unwrap_or(u64::MAX))
                .fold(0_u64, u64::saturating_add)
        })
}
