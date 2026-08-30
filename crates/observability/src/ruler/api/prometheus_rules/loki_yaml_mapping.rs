use super::*;

pub(crate) fn loki_yaml_mapping(value: &serde_yaml::Value) -> Option<&serde_yaml::Mapping> {
    match value {
        serde_yaml::Value::Mapping(fields) => Some(fields),
        _ => None,
    }
}
