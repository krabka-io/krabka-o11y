
pub(crate) fn u64_limit_from_usize(value: usize) -> u64 {
    if value == usize::MAX {
        0
    } else {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}
