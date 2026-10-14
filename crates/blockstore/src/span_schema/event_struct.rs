use super::{DataType, Field, Fields, nested_attribute_fields};

pub(crate) fn event_struct() -> DataType {
    let mut fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("time_since_start_nano", DataType::Int64, true),
    ];
    fields.extend(nested_attribute_fields());
    DataType::Struct(Fields::from(fields))
}
