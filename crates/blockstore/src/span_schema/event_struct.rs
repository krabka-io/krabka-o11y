use super::*;

pub(crate) fn event_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("time_since_start_nano", DataType::Int64, true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}
