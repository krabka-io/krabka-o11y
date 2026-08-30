use super::*;

pub(crate) fn normalize_loki_vector_sample_timestamps_to_seconds(value: &mut Value) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(sample) = series.get_mut("value").and_then(Value::as_array_mut) else {
            continue;
        };
        let Some(timestamp) = sample.get_mut(0) else {
            continue;
        };
        *timestamp = match timestamp {
            Value::Number(number) => {
                let seconds = unix_ns_string_to_loki_seconds(&number.to_string());
                json!(seconds)
            }
            Value::String(text) => json!(unix_ns_string_to_loki_seconds(text)),
            _ => continue,
        };
    }
}
