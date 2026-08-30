use super::{Bytes, HttpQueryError, form_body_query};

pub(crate) fn request_query_or_form_body(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    match raw_query {
        Some(raw_query) if !raw_query.is_empty() => Ok(raw_query.to_string()),
        _ if !body.is_empty() => form_body_query(body),
        _ => Err(HttpQueryError::MissingQueryParameter("query")),
    }
}
