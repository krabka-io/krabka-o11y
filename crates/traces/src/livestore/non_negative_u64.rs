
pub(crate) fn non_negative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
