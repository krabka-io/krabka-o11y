use super::{Value, json, loki_yaml_mapping, serde_yaml_key};

pub(crate) fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
    let values = fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            let mut decoded = serde_json::Map::new();
            for entry in values {
                let Some(key) = entry.0.as_str() else {
                    continue;
                };
                let Some(value) = entry.1.as_str() else {
                    continue;
                };
                decoded.insert(key.to_string(), json!(value));
            }
            decoded
        })
        .unwrap_or_default();
    Value::Object(values)
}
