use super::*;

pub(crate) fn yaml_string(value: &serde_yaml::Value, key: &str) -> String {
    yaml_optional_string(value, key).unwrap_or_default()
}
