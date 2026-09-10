pub(crate) fn parse_positive_u32(value: &str) -> Result<u32, String> {
    match value.parse::<u32>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        Ok(_) => Err("value must be at least 1".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
