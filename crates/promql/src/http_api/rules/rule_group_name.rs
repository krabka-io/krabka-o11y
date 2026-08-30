use super::*;

pub(crate) fn rule_group_name(group: &serde_yaml::Value) -> Result<String, ApiError> {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain a non-empty name"))
}
