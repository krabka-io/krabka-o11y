pub(crate) fn parse_min_two_usize(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 2 => Ok(parsed),
        Ok(_) => Err("value must be at least 2".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
