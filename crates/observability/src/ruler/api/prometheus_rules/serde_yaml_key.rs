use super::*;

pub(crate) fn serde_yaml_key(value: &'static str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_string())
}
