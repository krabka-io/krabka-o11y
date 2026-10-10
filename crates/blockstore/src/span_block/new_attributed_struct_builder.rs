use super::{
    Arc, ArrayBuilder, DataType, Field, Fields, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, StringBuilder,
    StructBuilder, new_str_list, new_str_list_list,
};

/// One column of a struct: its field and the builder that fills it.
pub(crate) struct StructColumn {
    pub(crate) field: Field,
    pub(crate) builder: Box<dyn ArrayBuilder>,
}

/// A struct builder for a span event or link: its two leading columns, then
/// the attribute keys, values and typed attributes every such struct carries.
pub(crate) fn new_attributed_struct_builder(leading: [StructColumn; 2]) -> StructBuilder {
    let [first, second] = leading;
    StructBuilder::new(
        Fields::from(vec![
            first.field,
            second.field,
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
            Field::new("attr_typed", DataType::Utf8, true),
        ]),
        vec![
            first.builder,
            second.builder,
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
            Box::new(StringBuilder::new()),
        ],
    )
}
