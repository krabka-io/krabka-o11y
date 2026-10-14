use super::{Value, json, yaml_string_entries};

pub(crate) fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
    let values = yaml_string_entries(fields, name)
        .into_iter()
        .map(|(key, value)| (key, json!(value)))
        .collect();
    Value::Object(values)
}
