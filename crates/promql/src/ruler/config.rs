use std::collections::BTreeMap;

use krabka_units::prelude::*;

use crate::PromqlError;

mod parse_duration;
mod stable_hash_parts;

pub(super) use parse_duration::parse_duration;
pub(super) use stable_hash_parts::stable_hash_parts;

pub(super) use crate::yaml_optional_string;

pub(crate) fn yaml_required_string(
    value: &serde_yaml::Value,
    key: &str,
) -> Result<String, PromqlError> {
    yaml_optional_string(value, key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PromqlError::Exec(format!("recording rule must contain a non-empty {key}")))
}

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

pub(crate) fn yaml_string_map(value: &serde_yaml::Value, key: &str) -> BTreeMap<String, String> {
    let Some(mapping) = value.get(key).and_then(serde_yaml::Value::as_mapping) else {
        return BTreeMap::new();
    };
    let mut result = BTreeMap::new();
    for yaml_key in mapping.keys() {
        let Some(string_key) = yaml_key.as_str() else {
            continue;
        };
        let Some(string_value) = mapping.get(yaml_key).and_then(serde_yaml::Value::as_str) else {
            continue;
        };
        result.insert(string_key.to_owned(), string_value.to_owned());
    }
    result
}
