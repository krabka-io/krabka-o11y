use super::{
    DataType, Field, Int64Builder, StringBuilder, StructBuilder, StructColumn,
    new_attributed_struct_builder,
};

pub(crate) fn new_event_struct_builder() -> StructBuilder {
    new_attributed_struct_builder([
        StructColumn {
            field: Field::new("name", DataType::Utf8, true),
            builder: Box::new(StringBuilder::new()),
        },
        StructColumn {
            field: Field::new("time_since_start_nano", DataType::Int64, true),
            builder: Box::new(Int64Builder::new()),
        },
    ])
}
