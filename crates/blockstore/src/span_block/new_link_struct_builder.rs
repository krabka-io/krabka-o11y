use super::{
    Arc, DataType, Field, Fields, FixedSizeBinaryBuilder, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE,
    StructBuilder, new_str_list, new_str_list_list,
};

pub(crate) fn new_link_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
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
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(8)),
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
        ],
    )
}
