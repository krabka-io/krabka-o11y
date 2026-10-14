use super::Uri;

pub(crate) fn required_seconds(uri: &Uri, key: &str) -> Result<i64, String> {
    crate::querier::http::required_seconds_param(uri, key)
}
