use super::*;

pub(crate) fn span_list_type() -> DataType {
    let struct_fields = Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ]);

    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields),
        false,
    )))
}
