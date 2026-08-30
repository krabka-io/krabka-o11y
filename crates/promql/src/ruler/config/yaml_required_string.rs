use super::{PromqlError, yaml_optional_string};

pub(crate) fn yaml_required_string(
    value: &serde_yaml::Value,
    key: &str,
) -> Result<String, PromqlError> {
    yaml_optional_string(value, key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PromqlError::Exec(format!("recording rule must contain a non-empty {key}")))
}
