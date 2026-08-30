use super::BackendError;

/// Build a `host/path?query` URL with the given params.
///
/// The crate builds `reqwest` without the `query` feature, so `url` encodes the
/// query strings. The legacy query-frontend uses the same approach.
pub(crate) fn build_url(base: &str, params: &[(&str, String)]) -> Result<reqwest::Url, BackendError> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|e| BackendError::Transport(format!("invalid url {base}: {e}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in params {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}
