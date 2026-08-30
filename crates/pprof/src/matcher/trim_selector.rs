use super::ProfileError;

pub(crate) fn trim_selector(input: &str) -> Result<&str, ProfileError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok("");
    }
    if let Some(inner) = trimmed.strip_prefix('{') {
        return inner
            .strip_suffix('}')
            .ok_or_else(|| ProfileError::Plan("unclosed label selector".to_string()));
    }
    if trimmed.ends_with('}') {
        return Err(ProfileError::Plan(
            "label selector has closing brace without opening brace".to_string(),
        ));
    }
    Ok(trimmed)
}
