use super::{AttrValue, DataType};

pub(crate) fn attr_data_type(value: &AttrValue) -> DataType {
    match value {
        AttrValue::Str(_) => DataType::Utf8,
        AttrValue::Int(_) => DataType::Int64,
        AttrValue::Float(_) => DataType::Float64,
        AttrValue::Bool(_) => DataType::Boolean,
    }
}
