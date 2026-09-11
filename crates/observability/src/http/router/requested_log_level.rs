use super::{Bytes, HttpQueryError, form_body_query, parse_log_level_param};

/// The level a `POST /log_level` is asking for.
///
/// `Loki` accepts the parameter in the query string, in a form body, or in
/// both. When both carry one the BODY wins, because the merged string puts it
/// first and the parser returns on the first match -- an ordering that only
/// shows when the two disagree.
///
/// # Errors
/// Returns [`HttpQueryError::MissingQueryParameter`] when neither source names
/// a level and [`HttpQueryError::InvalidQueryParameter`] when the level named
/// is not one this process filters at.
pub(crate) fn requested_log_level(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    // Both `!raw_query.is_empty()` guards are permanent mutation survivors
    // against `true`, and only against `true`. An empty query string with an
    // empty body falls through to the same empty string either way; with a
    // non-empty body it would merely append a trailing `&`, which the
    // parameter parser skips. Dropping them the other way, to `false`, does
    // change the answer: a level named only in the query string is lost.
    let raw_params = match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => raw_query.to_owned(),
        (Some(raw_query), false) if !raw_query.is_empty() => format!("{body_query}&{raw_query}"),
        _ => body_query,
    };
    parse_log_level_param(Some(&raw_params))
}
