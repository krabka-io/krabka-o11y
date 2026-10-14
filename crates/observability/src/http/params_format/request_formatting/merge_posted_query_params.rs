use super::{Bytes, HttpQueryError, form_body_query};

/// Which half of a POST query comes first once its URL query string and form
/// body are merged.
#[derive(Clone, Copy)]
pub(crate) enum PostedQueryParamsOrder {
    /// The URL query string, then the form body.
    UrlFirst,
    /// The form body, then the URL query string.
    BodyFirst,
}

/// Merges a POST query's URL query string with its form body in `order`.
///
/// The first arm's guard is a permanent mutation survivor against `true`:
/// dropping it lets an empty `raw_query` take that arm, and it is only reached
/// when the body is empty too, so the arm returns the same empty string the
/// fall-through would have.
pub(crate) fn merge_posted_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
    order: PostedQueryParamsOrder,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    if let Some(raw_query) = raw_query.filter(|query| !query.is_empty()) {
        if body_query.is_empty() {
            Ok(raw_query.to_owned())
        } else {
            Ok(match order {
                PostedQueryParamsOrder::UrlFirst => format!("{raw_query}&{body_query}"),
                PostedQueryParamsOrder::BodyFirst => format!("{body_query}&{raw_query}"),
            })
        }
    } else {
        Ok(body_query)
    }
}
