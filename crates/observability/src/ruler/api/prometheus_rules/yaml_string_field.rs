use super::*;

pub(crate) fn yaml_string_field<'a>(
    fields: &'a serde_yaml::Mapping,
    name: &'static str,
) -> Option<&'a str> {
    fields
        .get(serde_yaml_key(name))
        .and_then(serde_yaml::Value::as_str)
}
