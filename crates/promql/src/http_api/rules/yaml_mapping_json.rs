use super::{Map, Value, json};

pub(crate) fn yaml_mapping_json(value: &serde_yaml::Value, key: &str) -> Value {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map_or_else(
            || json!({}),
            |mapping| {
                let object = mapping
                    .iter()
                    .filter_map(|(name, value)| {
                        Some((
                            name.as_str()?.to_string(),
                            Value::String(value.as_str().unwrap_or_default().to_string()),
                        ))
                    })
                    .collect::<Map<_, _>>();
                Value::Object(object)
            },
        )
}
