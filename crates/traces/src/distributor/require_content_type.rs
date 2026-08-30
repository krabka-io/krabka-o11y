use super::{HeaderMap, TracesError, header};

pub(crate) fn require_content_type(headers: &HeaderMap, allowed: &[&str]) -> Result<(), TracesError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Ok(());
    };
    let declared = value
        .to_str()
        .map_err(|err| TracesError::UnsupportedContentType(err.to_string()))?;
    let media_type = declared
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if allowed
        .iter()
        .any(|allowed| media_type == allowed.to_ascii_lowercase())
    {
        Ok(())
    } else {
        Err(TracesError::UnsupportedContentType(declared.to_string()))
    }
}
