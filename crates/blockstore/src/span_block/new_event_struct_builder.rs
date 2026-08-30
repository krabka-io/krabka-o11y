use super::*;

pub(crate) fn new_event_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("time_since_start_nano", DataType::Int64, true),
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
        ],
    )
}
