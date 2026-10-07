/// Parses the row target a compacted block is left alone at, which is at least
/// one row.
///
/// A target of zero rows would seal every block on sight and the ladder would
/// compact nothing.
pub(crate) fn parse_compactor_target_rows(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        Ok(_) => Err("value must be at least 1".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
