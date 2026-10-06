use super::{AnyValue, AttrValue, OtlpValue};

pub(crate) fn attr_value_from_otlp(value: &AnyValue) -> Option<AttrValue> {
    if let Some(OtlpValue::ArrayValue(array)) = value.value.as_ref()
        && array.values.iter().any(|element| {
            !matches!(
                element.value,
                Some(
                    OtlpValue::StringValue(_)
                        | OtlpValue::IntValue(_)
                        | OtlpValue::DoubleValue(_)
                        | OtlpValue::BoolValue(_)
                )
            ) || array.values.first().is_some_and(|first| {
                first.value.as_ref().map(std::mem::discriminant)
                    != element.value.as_ref().map(std::mem::discriminant)
            })
        })
    {
        return Some(AttrValue::Unsupported(
            crate::span::AttrValue::encode_otlp_json(value).to_string(),
        ));
    }
    match value.value.as_ref()? {
        OtlpValue::StringValue(value) => Some(AttrValue::Str(value.clone())),
        OtlpValue::IntValue(value) => Some(AttrValue::Int(*value)),
        OtlpValue::DoubleValue(value) => Some(AttrValue::Float(*value)),
        OtlpValue::BoolValue(value) => Some(AttrValue::Bool(*value)),
        OtlpValue::BytesValue(_) | OtlpValue::KvlistValue(_) => Some(AttrValue::Unsupported(
            crate::span::AttrValue::encode_otlp_json(value).to_string(),
        )),
        OtlpValue::ArrayValue(array) => Some(AttrValue::Array(
            array
                .values
                .iter()
                .map(attr_value_from_otlp)
                .collect::<Option<Vec<_>>>()?,
        )),
        OtlpValue::StringValueStrindex(_) => None,
    }
}
