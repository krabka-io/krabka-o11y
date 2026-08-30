use super::*;

pub(crate) fn otlp_value(value: &AttrValue) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(match value {
            AttrValue::Str(value) => OtlpValue::StringValue(value.clone()),
            AttrValue::Int(value) => OtlpValue::IntValue(*value),
            AttrValue::Float(value) => OtlpValue::DoubleValue(*value),
            AttrValue::Bool(value) => OtlpValue::BoolValue(*value),
        }),
    }
}
