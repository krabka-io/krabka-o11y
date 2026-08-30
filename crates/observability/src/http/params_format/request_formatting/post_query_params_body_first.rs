use super::*;

/// Merges a POST query's URL query string with its form body, body first.
///
/// Its first arm's guard is a permanent survivor for the same reason as
/// [`post_query_params`].
pub(crate) fn post_query_params_body_first(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{body_query}&{raw_query}"))
        }
        _ => Ok(body_query),
    }
}
