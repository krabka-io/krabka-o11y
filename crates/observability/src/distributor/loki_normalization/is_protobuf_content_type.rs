use super::*;

pub(crate) fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    content_type.split(';').next().is_some_and(|content_type| {
        matches!(
            content_type.trim(),
            "application/x-protobuf" | "application/protobuf"
        )
    })
}
