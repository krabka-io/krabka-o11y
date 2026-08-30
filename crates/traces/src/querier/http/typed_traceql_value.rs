use super::{TraceqlValue, TypedValue};

pub(crate) fn typed_traceql_value(value: &TraceqlValue) -> Option<TypedValue> {
    let (type_, value) = match value {
        TraceqlValue::Str(value) => ("string", value.clone()),
        TraceqlValue::Int(value) | TraceqlValue::Duration(value) => ("int", value.to_string()),
        TraceqlValue::Float(value) if value.is_finite() => ("float", value.to_string()),
        TraceqlValue::Bool(value) => ("bool", value.to_string()),
        TraceqlValue::Float(_) | TraceqlValue::Nil => return None,
    };
    Some(TypedValue {
        type_: type_.into(),
        value,
    })
}
