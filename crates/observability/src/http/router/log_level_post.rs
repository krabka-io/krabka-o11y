use super::*;

pub(crate) async fn log_level_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let body_query = match form_body_query(&body) {
        Ok(body_query) => body_query,
        Err(error) => return error.into_response(),
    };
    // Both `!raw_query.is_empty()` guards are permanent mutation survivors
    // against `true`, and only against `true`. An empty query string with an
    // empty body falls through to the same empty string either way; with a
    // non-empty body it would merely append a trailing `&`, which the
    // parameter parser skips. Dropping them the other way, to `false`, does
    // change the answer: a level named only in the query string is lost.
    let raw_params = match (raw_query.as_deref(), body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => raw_query.to_owned(),
        (Some(raw_query), false) if !raw_query.is_empty() => format!("{body_query}&{raw_query}"),
        _ => body_query,
    };
    match parse_log_level_param(Some(&raw_params)) {
        Ok(level) => json_response(
            StatusCode::OK,
            &json!({
                "status": "success",
                "message": format!("Log level set to {level}"),
            }),
        ),
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            value,
        }) => log_level_failed_response(&format!("unrecognized log level \"{value}\"")),
        Err(HttpQueryError::MissingQueryParameter("log_level")) => {
            log_level_failed_response("unrecognized log level \"\"")
        }
        Err(error) => error.into_response(),
    }
}
