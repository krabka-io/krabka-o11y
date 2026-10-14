use super::{Map, Value, json};

pub(crate) fn yaml_mapping_json(value: &serde_yaml::Value, key: &str) -> Value {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map_or_else(
            || json!({}),
            |mapping| {
                let mut object = Map::new();
                for entry in mapping {
                    let Some(name) = entry.0.as_str() else {
                        continue;
                    };
                    object.insert(
                        name.to_string(),
                        Value::String(entry.1.as_str().unwrap_or_default().to_string()),
                    );
                }
                Value::Object(object)
            },
        )
}
