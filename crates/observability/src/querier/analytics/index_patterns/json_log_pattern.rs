use super::{Value, json_value_pattern};

/// Templatizes a single-object JSON log line. Returns `None` for anything that
/// is not a JSON object, so the caller falls back to whitespace or logfmt
/// mining.
pub(crate) fn json_log_pattern(line: &str) -> Option<String> {
    // `from_str` already rejects non-objects and non-JSON, so there is no
    // cheap pre-check guard here: a leading-`{` fast path would be a pure
    // performance optimization with no behavior of its own to test.
    let Value::Object(map) = serde_json::from_str::<Value>(line.trim()).ok()? else {
        return None;
    };
    let templatized = Value::Object(
        map.iter()
            .map(|(key, value)| (key.clone(), json_value_pattern(value)))
            .collect(),
    );
    serde_json::to_string(&templatized).ok()
}
