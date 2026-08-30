use super::{AttrValue, KeyValue};

pub(crate) fn traceql_attr(attr: &KeyValue) -> Option<krabka_traceql::AttrValue> {
    Some(match &attr.value {
        AttrValue::Str(value) => krabka_traceql::AttrValue::Str(value.clone()),
        AttrValue::Int(value) => krabka_traceql::AttrValue::Int(*value),
        AttrValue::Double(value) => krabka_traceql::AttrValue::Float(*value),
        AttrValue::Bool(value) => krabka_traceql::AttrValue::Bool(*value),
        AttrValue::Bytes(_) => return None,
    })
}
