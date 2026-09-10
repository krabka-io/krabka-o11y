pub(crate) fn parse_external_label(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "external labels must use name=value".to_string())?;
    if name.is_empty() || value.is_empty() {
        return Err("external labels must have a non-empty name and value".to_string());
    }
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(format!("invalid Prometheus label name {name:?}"));
    }
    Ok((name.to_string(), value.to_string()))
}
