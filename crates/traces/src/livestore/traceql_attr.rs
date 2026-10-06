use super::{AttrValue, KeyValue};

pub(crate) fn traceql_attr(attr: &KeyValue) -> Option<krabka_traceql::AttrValue> {
    convert_value(&attr.value)
}

fn convert_value(value: &AttrValue) -> Option<krabka_traceql::AttrValue> {
    if let AttrValue::Array(values) = value
        && values.iter().any(|element| {
            matches!(
                element,
                AttrValue::Array(_) | AttrValue::Bytes(_) | AttrValue::Unsupported(_)
            ) || values.first().is_some_and(|first| {
                std::mem::discriminant(first) != std::mem::discriminant(element)
            })
        })
    {
        return Some(krabka_traceql::AttrValue::Unsupported(
            value.otlp_json().to_string(),
        ));
    }
    Some(match value {
        AttrValue::Unsupported(value) => krabka_traceql::AttrValue::Unsupported(value.clone()),
        AttrValue::Array(values) => krabka_traceql::AttrValue::Array(
            values
                .iter()
                .map(convert_value)
                .collect::<Option<Vec<_>>>()?,
        ),
        AttrValue::Str(value) => krabka_traceql::AttrValue::Str(value.clone()),
        AttrValue::Int(value) => krabka_traceql::AttrValue::Int(*value),
        AttrValue::Double(value) => krabka_traceql::AttrValue::Float(*value),
        AttrValue::Bool(value) => krabka_traceql::AttrValue::Bool(*value),
        AttrValue::Bytes(_) => {
            krabka_traceql::AttrValue::Unsupported(value.otlp_json().to_string())
        }
    })
}
