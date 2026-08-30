use super::*;

pub(crate) fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                let media_type = part.split(';').next().unwrap_or_default().trim();
                media_type.eq_ignore_ascii_case("application/protobuf")
                    || media_type.eq_ignore_ascii_case("application/x-protobuf")
            })
        })
}
