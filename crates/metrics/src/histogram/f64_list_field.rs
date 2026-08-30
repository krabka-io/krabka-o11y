use super::{DataType, Field};

pub(crate) fn f64_list_field() -> Field {
    Field::new("item", DataType::Float64, false)
}
