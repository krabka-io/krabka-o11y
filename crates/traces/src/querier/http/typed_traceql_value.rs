use super::{TraceqlValue, TypedValue};

pub(crate) fn typed_traceql_value(value: &TraceqlValue) -> Option<TypedValue> {
    let typed = match value {
        TraceqlValue::Str(value) => TypedValue {
            type_: "string".into(),
            value: value.clone(),
        },
        TraceqlValue::Int(value) | TraceqlValue::Duration(value) => TypedValue {
            type_: "int".into(),
            value: value.to_string(),
        },
        TraceqlValue::Float(value) if value.is_finite() => TypedValue {
            type_: "float".into(),
            value: value.to_string(),
        },
        TraceqlValue::Bool(value) => TypedValue {
            type_: "bool".into(),
            value: value.to_string(),
        },
        TraceqlValue::Array(_) | TraceqlValue::Float(_) | TraceqlValue::Nil => return None,
    };
    Some(typed)
}
