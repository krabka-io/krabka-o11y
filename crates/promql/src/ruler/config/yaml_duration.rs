use super::{PromqlError, Time, TimeExt, parse_duration, yaml_optional_string};

/// Parses a Prometheus duration for the given rule field.
///
/// This function returns a hard error to the caller for a malformed value. A
/// missing field is a zero extent, with no duration. This function rejects an
/// empty, negative, or unparseable value. It does not coerce that value to
/// zero, because zero would make `for` and `interval` fire immediately.
pub(crate) fn yaml_duration(value: &serde_yaml::Value, key: &str) -> Result<Time, PromqlError> {
    match yaml_optional_string(value, key) {
        Some(duration) => parse_duration(&duration),
        None => Ok(Time::ZERO),
    }
}
