use super::{HeaderMap, header};

pub(crate) fn is_jaeger_binary_thrift(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|declared| declared.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/vnd.apache.thrift.binary")
        })
}
