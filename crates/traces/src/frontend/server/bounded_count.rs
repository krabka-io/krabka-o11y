use super::{Uri, query_param};

pub(crate) fn bounded_count(uri: &Uri, key: &str, default: usize) -> usize {
    query_param(uri, key)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
