use super::Uri;

pub(crate) fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|pair| (pair.0 == key).then(|| pair.1.into_owned()))
}
