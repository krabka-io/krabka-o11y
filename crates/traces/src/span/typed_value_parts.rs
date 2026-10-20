use super::AttrValue;

/// What an attribute value contributes to Tempo's tag-value listing.
pub(crate) enum TypedPart<'a, V> {
    /// An array, whose elements each contribute on their own.
    Array(&'a [V]),
    /// A scalar, as its Tempo type name and rendered value.
    Scalar(krabka_traceql::TypedValue),
    /// A value Tempo does not list.
    Unlisted,
}

/// An attribute value that [`typed_value_parts`] can flatten.
pub(crate) trait TypedValue: Sized {
    fn typed_part(&self) -> TypedPart<'_, Self>;
}

/// Flatten `value` into Tempo's `(type, value)` tag-value pairs, recursing
/// into arrays and dropping the values Tempo does not list.
pub(crate) fn typed_value_parts<V: TypedValue>(value: &V) -> Vec<krabka_traceql::TypedValue> {
    match value.typed_part() {
        TypedPart::Array(values) => values.iter().flat_map(typed_value_parts).collect(),
        TypedPart::Scalar(value) => vec![value],
        TypedPart::Unlisted => Vec::new(),
    }
}

impl TypedValue for AttrValue {
    fn typed_part(&self) -> TypedPart<'_, Self> {
        match self {
            AttrValue::Array(values) => TypedPart::Array(values),
            AttrValue::Str(value) => {
                TypedPart::Scalar(krabka_traceql::TypedValue::new("string", value.clone()))
            }
            AttrValue::Int(value) => {
                TypedPart::Scalar(krabka_traceql::TypedValue::new("int", value.to_string()))
            }
            AttrValue::Double(value) => {
                TypedPart::Scalar(krabka_traceql::TypedValue::new("float", value.to_string()))
            }
            AttrValue::Bool(value) => {
                TypedPart::Scalar(krabka_traceql::TypedValue::new("bool", value.to_string()))
            }
            AttrValue::Bytes(value) => TypedPart::Scalar(krabka_traceql::TypedValue::new(
                "string",
                hex::encode(value),
            )),
            AttrValue::Unsupported(_) => TypedPart::Unlisted,
        }
    }
}
