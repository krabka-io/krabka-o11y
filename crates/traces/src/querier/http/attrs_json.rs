use super::{Attribute, Value, attr_values_json, group_attrs, json};

pub(crate) fn attrs_json(attrs: &[Attribute]) -> Value {
    Value::Array(
        group_attrs(attrs)
            .into_iter()
            .map(|group| {
                json!({
                    "key": group.key,
                    "value": attr_values_json(&group.values),
                })
            })
            .collect(),
    )
}
