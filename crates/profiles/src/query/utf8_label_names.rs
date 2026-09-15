use super::HeaderMap;

pub(crate) fn client_allows_utf8_label_names(headers: &HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .flat_map(|media_type| media_type.split(';').skip(1))
        .filter_map(|parameter| parameter.trim().split_once('='))
        .any(|(name, value)| {
            name.trim().eq_ignore_ascii_case("allow-utf8-labelnames") && value.trim() == "true"
        })
}

pub(crate) fn is_legacy_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_utf8_capability_and_legacy_names() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::ACCEPT,
            "application/json, */*; allow-utf8-labelnames=true"
                .parse()
                .unwrap(),
        );
        assert!(client_allows_utf8_label_names(&headers));
        headers.insert(
            axum::http::header::ACCEPT,
            "*/*; allow-utf8-labelnames=TRUE".parse().unwrap(),
        );
        assert!(!client_allows_utf8_label_names(&headers));
        assert!(is_legacy_label_name("service_name"));
        assert!(!is_legacy_label_name("service.name"));
        assert!(!is_legacy_label_name("世界"));
    }
}
