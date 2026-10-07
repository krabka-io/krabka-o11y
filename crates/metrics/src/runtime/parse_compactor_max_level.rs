/// Parses the compaction ladder height, which is at least one level.
///
/// A ladder of zero levels leaves every block at its cap, so nothing is ever an
/// input and the compactor merges nothing.
pub(crate) fn parse_compactor_max_level(value: &str) -> Result<u32, String> {
    match value.parse::<u32>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        Ok(_) => Err("value must be at least 1".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
