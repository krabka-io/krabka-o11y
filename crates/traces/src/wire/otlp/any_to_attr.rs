use super::*;

pub(crate) fn any_to_attr(value: &AnyValue) -> Option<AttrValue> {
    match value.value.as_ref()? {
        Value::StringValue(value) => Some(AttrValue::Str(value.clone())),
        Value::StringValueStrindex(value) => Some(AttrValue::Str(format!("strindex:{value}"))),
        Value::IntValue(value) => Some(AttrValue::Int(*value)),
        Value::DoubleValue(value) => Some(AttrValue::Double(*value)),
        Value::BoolValue(value) => Some(AttrValue::Bool(*value)),
        Value::BytesValue(value) => Some(AttrValue::Bytes(value.clone())),
        Value::ArrayValue(_) => None,
        Value::KvlistValue(value) => Some(AttrValue::Str(format!(
            "{{{}}}",
            value
                .values
                .iter()
                .filter_map(|kv| Some(format!("{}:{}", kv.key, any_to_text(kv.value.as_ref()?)?)))
                .collect::<Vec<_>>()
                .join(",")
        ))),
    }
}
