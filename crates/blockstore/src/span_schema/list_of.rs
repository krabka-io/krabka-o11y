use super::*;

pub(crate) fn list_of(name: &str, inner: DataType, nullable: bool) -> DataType {
    DataType::List(Arc::new(Field::new(name, inner, nullable)))
}
