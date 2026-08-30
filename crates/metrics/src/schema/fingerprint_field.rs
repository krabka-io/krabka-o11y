use super::*;

pub(crate) fn fingerprint_field() -> Field {
    Field::new(COL_FINGERPRINT, DataType::UInt64, false)
}
