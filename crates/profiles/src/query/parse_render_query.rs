use super::{ProfileError, parse_label_selector};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedRenderQuery {
    pub(crate) profile_type: String,
    pub(crate) selector: String,
}

pub(crate) fn parse_render_query(query: &str) -> Result<ParsedRenderQuery, ProfileError> {
    let trimmed = query.trim();
    let Some(open) = trimmed.find('{') else {
        if trimmed.is_empty() {
            return Err(ProfileError::Plan("missing query".to_string()));
        }
        return Ok(ParsedRenderQuery {
            profile_type: trimmed.to_string(),
            selector: "{}".to_string(),
        });
    };
    let profile_type = trimmed[..open].trim();
    let selector = &trimmed[open..];
    if profile_type.is_empty() {
        return Err(ProfileError::Plan("missing profile type".to_string()));
    }
    parse_label_selector(selector)?;
    Ok(ParsedRenderQuery {
        profile_type: profile_type.to_string(),
        selector: selector.to_string(),
    })
}
