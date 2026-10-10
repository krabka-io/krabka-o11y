use super::{Bytes, HttpQueryError, PostedQueryParamsOrder, merge_posted_query_params};

/// Merges a POST query's URL query string with its form body, URL first.
pub(crate) fn post_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    merge_posted_query_params(raw_query, body, PostedQueryParamsOrder::UrlFirst)
}
