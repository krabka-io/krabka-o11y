use super::{KeyValue, OtlpKv, any_to_attr};

pub(crate) fn kv_to_attrs(attr: &OtlpKv) -> Vec<KeyValue> {
    let Some(value) = attr.value.as_ref() else {
        return Vec::new();
    };
    vec![KeyValue {
        key: attr.key.clone(),
        value: any_to_attr(value),
    }]
}
