use super::*;

pub(crate) fn list_list_of(inner: DataType) -> DataType {
    list_of("item", list_of("item", inner, true), true)
}
