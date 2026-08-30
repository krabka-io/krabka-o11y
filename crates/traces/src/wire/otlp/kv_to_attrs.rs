use super::{KeyValue, OtlpKv, Value, any_to_attr};

pub(crate) fn kv_to_attrs(attr: &OtlpKv) -> Vec<KeyValue> {
    let Some(value) = attr.value.as_ref() else {
        return Vec::new();
    };
    match value.value.as_ref() {
        Some(Value::ArrayValue(array)) => array
            .values
            .iter()
            .filter_map(|value| {
                Some(KeyValue {
                    key: attr.key.clone(),
                    value: any_to_attr(value)?,
                })
            })
            .collect(),
        _ => any_to_attr(value)
            .map(|value| {
                vec![KeyValue {
                    key: attr.key.clone(),
                    value,
                }]
            })
            .unwrap_or_default(),
    }
}
