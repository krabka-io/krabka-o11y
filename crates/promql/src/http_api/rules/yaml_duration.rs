use super::{ Time, TimeExt, parse_yaml_duration};

/// Returns the duration at a rule-file key as an extent.
///
/// The keys are `for:` and the `interval:` of a group. This function returns a
/// zero extent for an absent or unparseable value.
pub(crate) fn yaml_duration(value: &serde_yaml::Value, key: &str) -> Time {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .and_then(parse_yaml_duration)
        .unwrap_or(Time::ZERO)
}
