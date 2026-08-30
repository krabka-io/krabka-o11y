use super::{DataType, Field};

pub(crate) fn utf8_map_field(name: &str, nullable: bool) -> Field {
    Field::new_map(
        name,
        "entries",
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        false,
        nullable,
    )
}
