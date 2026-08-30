
pub(crate) fn parse_nanos(s: &str) -> i128 {
    s.parse().unwrap_or(i128::MIN)
}
