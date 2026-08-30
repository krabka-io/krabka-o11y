use super::*;

/// `log_level_post` accepts the level in a query string, a form body, or
/// both. When both carry one the BODY wins, because the merged string puts
/// it first and the parser returns on the first match -- an ordering that
/// only shows when the two disagree.
#[tokio::test]
pub(crate) async fn a_log_level_post_prefers_the_body_over_the_query_string() {
    use axum::response::IntoResponse as _;

    let post = |query: Option<&str>, body: &str| {
        let query = query.map(str::to_string);
        let body = axum::body::Bytes::from(body.to_string());
        async move {
            let response = super::super::prelude::log_level_post(axum::extract::RawQuery(query), body)
                .await
                .into_response();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("the response body is readable");
            (status, String::from_utf8(bytes.to_vec()).expect("utf-8"))
        }
    };

    // Either source alone.
    let (status, body) = post(Some("log_level=debug"), "").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to debug"));

    let (status, body) = post(None, "log_level=info").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to info"));

    // Both, disagreeing: the body wins.
    let (status, body) = post(Some("log_level=warn"), "log_level=info").await;
    check!(status == axum::http::StatusCode::OK);
    check!(
        body.contains("Log level set to info"),
        "the body's level, not the query string's: {body}"
    );

    // A body that carries no level at all, alongside a query string that
    // does. Every case above has the level in the body whenever the body is
    // non-empty, so the merge could have dropped the query string entirely
    // and they would all still pass.
    let (status, body) = post(Some("log_level=debug"), "other=1").await;
    check!(status == axum::http::StatusCode::OK);
    check!(
        body.contains("Log level set to debug"),
        "the query string supplies what the body lacks: {body}"
    );

    // An empty query string alongside a body is not a source.
    let (status, body) = post(Some(""), "log_level=error").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to error"));

    // Neither source, and an unrecognised level, are refused distinctly.
    let (status, body) = post(None, "").await;
    check!(status != axum::http::StatusCode::OK);
    check!(body.contains("unrecognized log level"));

    let (_, body) = post(Some("log_level=verbose"), "").await;
    check!(
        body.contains("verbose"),
        "the refusal names what was sent: {body}"
    );
}
