use super::{DataType, Field, Fields, nested_attribute_fields};

pub(crate) fn link_struct() -> DataType {
    let mut fields = vec![
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
    ];
    fields.extend(nested_attribute_fields());
    DataType::Struct(Fields::from(fields))
}
