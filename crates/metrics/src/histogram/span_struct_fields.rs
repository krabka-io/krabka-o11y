use super::*;

pub(crate) fn span_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ])
}
