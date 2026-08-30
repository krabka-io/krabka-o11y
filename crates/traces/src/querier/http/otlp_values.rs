use super::{AttrValue, OtlpAnyValue, otlp_value, OtlpValue, OtlpArrayValue};

pub(crate) fn otlp_values(values: &[&AttrValue]) -> OtlpAnyValue {
    if let [value] = values {
        return otlp_value(value);
    }
    OtlpAnyValue {
        value: Some(OtlpValue::ArrayValue(OtlpArrayValue {
            values: values.iter().map(|value| otlp_value(value)).collect(),
        })),
    }
}
