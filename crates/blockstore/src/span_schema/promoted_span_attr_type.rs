use super::DataType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromotedSpanAttrType {
    String,
    Int,
    Double,
    Bool,
}

impl PromotedSpanAttrType {
    #[must_use]
    pub fn data_type(self) -> DataType {
        match self {
            Self::String => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            Self::Int => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Bool => DataType::Boolean,
        }
    }
}
