use super::DataType;

pub(crate) fn clock_label_dict() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}
