use super::Uri;

pub(crate) fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}
