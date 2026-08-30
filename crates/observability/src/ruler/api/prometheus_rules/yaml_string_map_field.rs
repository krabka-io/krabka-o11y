use super::{Value, json, loki_yaml_mapping, serde_yaml_key};

pub(crate) fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
    let values = fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), json!(value.as_str()?)))
                })
                .collect::<serde_json::Map<_, _>>()
        })
        .unwrap_or_default();
    Value::Object(values)
}
