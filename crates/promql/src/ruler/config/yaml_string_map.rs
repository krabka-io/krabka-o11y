use super::*;

pub(crate) fn yaml_string_map(value: &serde_yaml::Value, key: &str) -> BTreeMap<String, String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map(|mapping| {
            mapping
                .iter()
                .filter_map(|(key, value)| Some((key.as_str()?, value.as_str()?)))
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect()
        })
        .unwrap_or_default()
}
