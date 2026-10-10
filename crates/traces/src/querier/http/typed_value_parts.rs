use super::AttrValue;
use crate::span::typed_value_parts::{TypedPart, TypedValue};

impl TypedValue for AttrValue {
    fn typed_part(&self) -> TypedPart<'_, Self> {
        match self {
            AttrValue::Array(values) => TypedPart::Array(values),
            AttrValue::Str(value) => TypedPart::Scalar("string", value.clone()),
            AttrValue::Int(value) => TypedPart::Scalar("int", value.to_string()),
            AttrValue::Float(value) => TypedPart::Scalar("float", value.to_string()),
            AttrValue::Bool(value) => TypedPart::Scalar("bool", value.to_string()),
            AttrValue::Unsupported(_) => TypedPart::Unlisted,
        }
    }
}
