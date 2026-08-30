/// Builds the JSON number token for a millisecond timestamp.
///
/// This function mirrors Prometheus `MarshalTimestamp`. It writes the sign, then
/// the absolute integer seconds, then a millisecond fraction with the trailing
/// zeros trimmed when that fraction is non-zero.
pub(crate) fn format_timestamp_token(ts_ms: i64) -> String {
    let mut out = String::new();
    if ts_ms < 0 {
        out.push('-');
    }
    let magnitude = ts_ms.unsigned_abs();
    let seconds = magnitude / 1000;
    let fraction = magnitude % 1000;
    out.push_str(&seconds.to_string());
    if fraction != 0 {
        out.push('.');
        let padded = format!("{fraction:03}");
        out.push_str(padded.trim_end_matches('0'));
    }
    out
}
