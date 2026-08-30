use super::format_timestamp_token;

/// Encodes a millisecond timestamp as the JSON number Prometheus emits.
///
/// Prometheus emits this number from `jsonutil.MarshalTimestamp`. The number is
/// a bare integer for whole seconds, and otherwise a fraction with the trailing
/// zeros trimmed. Examples: `10`, `1435781430.781`, `-0.5`.
///
/// `serde_json` renders an `f64` of `10` as `10.0`. This function therefore
/// carries the value as a pre-formatted
/// [`RawValue`](serde_json::value::RawValue) number token, which keeps the
/// output byte-exact.
pub(crate) fn timestamp_seconds(ts_ms: i64) -> Box<serde_json::value::RawValue> {
    let token = format_timestamp_token(ts_ms);
    serde_json::value::RawValue::from_string(token)
        .expect("timestamp token is always valid JSON number syntax")
}
