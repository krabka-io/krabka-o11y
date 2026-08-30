/// Parses one numeric field of a corpus case, failing loudly rather than
/// leaving it unset. A field that will not parse is a mistake in the case
/// file, and treating it as absent removes the assertion it was written to
/// make.
pub(crate) fn parse_field<T: std::str::FromStr>(case: &str, key: &str, value: &str) -> T {
    value
        .parse()
        .unwrap_or_else(|_| panic!("{case}: `{key}` is not a valid value: {value:?}"))
}
