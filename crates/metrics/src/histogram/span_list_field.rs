use super::{Field, DataType, span_struct_fields};

pub(crate) fn span_list_field() -> Field {
    Field::new("item", DataType::Struct(span_struct_fields()), false)
}
