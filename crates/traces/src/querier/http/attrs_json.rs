use super::{AttrValue, Value, attr_values_json, group_attrs, json};

pub(crate) fn attrs_json(attrs: &[(String, AttrValue)]) -> Value {
    Value::Array(
        group_attrs(attrs)
            .into_iter()
            .map(|(key, values)| {
                json!({
                    "key": key,
                    "value": attr_values_json(&values),
                })
            })
            .collect(),
    )
}
