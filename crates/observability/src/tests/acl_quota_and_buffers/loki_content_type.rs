use super::*;

pub(crate) fn loki_content_type(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, value.parse().unwrap());
    headers
}
