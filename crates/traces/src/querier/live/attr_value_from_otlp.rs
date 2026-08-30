use super::*;

pub(crate) fn attr_value_from_otlp(value: &AnyValue) -> Option<AttrValue> {
    match value.value.as_ref()? {
        OtlpValue::StringValue(value) => Some(AttrValue::Str(value.clone())),
        OtlpValue::IntValue(value) => Some(AttrValue::Int(*value)),
        OtlpValue::DoubleValue(value) => Some(AttrValue::Float(*value)),
        OtlpValue::BoolValue(value) => Some(AttrValue::Bool(*value)),
        OtlpValue::BytesValue(value) => Some(AttrValue::Str(hex::encode(value))),
        OtlpValue::ArrayValue(array) => array.values.first().and_then(attr_value_from_otlp),
        OtlpValue::KvlistValue(_) | OtlpValue::StringValueStrindex(_) => None,
    }
}
