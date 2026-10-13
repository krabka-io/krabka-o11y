use super::{BackendError, QueryParam};

/// Build a `host/path?query` URL with the given params.
///
/// The crate builds `reqwest` without the `query` feature, so `url` encodes the
/// query strings. The legacy query-frontend uses the same approach.
pub(crate) fn build_url(base: &str, params: &[QueryParam]) -> Result<reqwest::Url, BackendError> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|e| BackendError::Transport(format!("invalid url {base}: {e}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        for param in params {
            pairs.append_pair(&param.key, &param.value);
        }
    }
    Ok(url)
}
