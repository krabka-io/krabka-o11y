use super::{
    DataType, Field, FixedSizeBinaryBuilder, StructBuilder, StructColumn,
    new_attributed_struct_builder,
};

pub(crate) fn new_link_struct_builder() -> StructBuilder {
    new_attributed_struct_builder([
        StructColumn {
            field: Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
            builder: Box::new(FixedSizeBinaryBuilder::new(16)),
        },
        StructColumn {
            field: Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
            builder: Box::new(FixedSizeBinaryBuilder::new(8)),
        },
    ])
}
