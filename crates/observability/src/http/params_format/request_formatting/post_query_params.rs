use super::{Bytes, HttpQueryError, form_body_query};

/// Merges a POST query's URL query string with its form body, URL first.
///
/// The first arm's guard is a permanent mutation survivor against `true`:
/// dropping it lets an empty `raw_query` take that arm, and it is only reached
/// when the body is empty too, so the arm returns the same empty string the
/// fall-through would have.
pub(crate) fn post_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{raw_query}&{body_query}"))
        }
        _ => Ok(body_query),
    }
}
