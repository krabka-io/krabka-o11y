use super::{Duration, Value, json};

pub(crate) fn unix_ns_string_to_loki_seconds(timestamp_ns: &str) -> Value {
    let timestamp_ns = timestamp_ns.parse::<u64>().unwrap_or_default();
    let seconds = timestamp_ns / 1_000_000_000;
    let nanos = timestamp_ns % 1_000_000_000;
    if nanos == 0 {
        json!(seconds)
    } else {
        json!(Duration::from_nanos(timestamp_ns).as_secs_f64())
    }
}
