use super::{DataType, Field, Fields, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, list_list_of, list_of};

pub(crate) fn link_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}
