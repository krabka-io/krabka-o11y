use super::{AttrValue, DataType};

pub(crate) fn attr_data_type(value: &AttrValue) -> DataType {
    match value {
        AttrValue::Int(_) => DataType::Int64,
        AttrValue::Float(_) => DataType::Float64,
        AttrValue::Bool(_) => DataType::Boolean,
        AttrValue::Str(_) | AttrValue::Unsupported(_) | AttrValue::Array(_) => DataType::Utf8,
    }
}
