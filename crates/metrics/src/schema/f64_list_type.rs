use super::{Arc, DataType, Field};

pub(crate) fn f64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, false)))
}
