use super::*;

pub(crate) fn loki_rule_group_name(rule_group: &serde_yaml::Value) -> Option<&str> {
    let serde_yaml::Value::Mapping(fields) = rule_group else {
        return None;
    };
    fields
        .get(serde_yaml::Value::String("name".to_string()))
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
}
