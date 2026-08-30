use super::*;

pub(crate) fn profile_type_dict() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}
