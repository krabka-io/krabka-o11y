pub(crate) fn parse_external_label(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "external labels must use name=value".to_string())?;
    if name.is_empty() || value.is_empty() {
        return Err("external labels must have a non-empty name and value".to_string());
    }
    Ok((name.to_string(), value.to_string()))
}
