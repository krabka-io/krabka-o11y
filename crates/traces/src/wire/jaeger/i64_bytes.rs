
pub(crate) fn i64_bytes(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}
