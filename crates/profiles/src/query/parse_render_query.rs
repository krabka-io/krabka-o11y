use super::{ProfileError, parse_label_selector};

pub(crate) fn parse_render_query(query: &str) -> Result<(String, String), ProfileError> {
    let trimmed = query.trim();
    let Some(open) = trimmed.find('{') else {
        if trimmed.is_empty() {
            return Err(ProfileError::Plan("missing query".to_string()));
        }
        return Ok((trimmed.to_string(), "{}".to_string()));
    };
    let profile_type = trimmed[..open].trim();
    let selector = &trimmed[open..];
    if profile_type.is_empty() {
        return Err(ProfileError::Plan("missing profile type".to_string()));
    }
    parse_label_selector(selector)?;
    Ok((profile_type.to_string(), selector.to_string()))
}
