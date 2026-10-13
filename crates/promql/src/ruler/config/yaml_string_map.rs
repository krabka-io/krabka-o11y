use super::BTreeMap;

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
