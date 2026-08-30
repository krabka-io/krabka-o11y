
pub(crate) fn nanos_to_millis(nanos: u64) -> i64 {
    i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX)
}
