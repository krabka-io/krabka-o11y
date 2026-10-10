use super::{Bytes, HttpQueryError, PostedQueryParamsOrder, merge_posted_query_params};
use crate::{SeriesParams, parse_series_params};

/// Merges a POST query's URL query string with its form body, body first.
pub(crate) fn post_query_params_body_first(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    merge_posted_query_params(raw_query, body, PostedQueryParamsOrder::BodyFirst)
}

/// Parses the series parameters of a metadata POST from its URL query string
/// and its form body, body first.
pub(crate) fn parse_posted_series_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<SeriesParams, HttpQueryError> {
    parse_series_params(Some(&post_query_params_body_first(raw_query, body)?))
}
