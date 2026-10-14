use super::{HeaderMap, header};

pub(crate) fn accepts_media_type(headers: &HeaderMap, media_types: &[&str]) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                let media_type = part.split(';').next().unwrap_or_default().trim();
                media_types
                    .iter()
                    .any(|expected| media_type.eq_ignore_ascii_case(expected))
            })
        })
}
