use super::*;

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
