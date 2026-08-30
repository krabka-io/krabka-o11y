use super::*;

pub(crate) fn timestamp_field() -> Field {
    Field::new(COL_TIMESTAMP, DataType::Int64, false)
}
