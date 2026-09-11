use super::*;

/// `requested_log_level` accepts the level in a query string, a form body, or
/// both. When both carry one the BODY wins, because the merged string puts
/// it first and the parser returns on the first match -- an ordering that
/// only shows when the two disagree.
#[test]
pub(crate) fn a_log_level_post_prefers_the_body_over_the_query_string() {
    let requested = |query: Option<&str>, body: &str| {
        super::super::prelude::requested_log_level(
            query,
            &axum::body::Bytes::from(body.to_string()),
        )
    };

    // Either source alone.
    check!(requested(Some("log_level=debug"), "").expect("a level is named") == "debug");
    check!(requested(None, "log_level=info").expect("a level is named") == "info");

    // Both, disagreeing: the body wins.
    check!(
        requested(Some("log_level=warn"), "log_level=info").expect("a level is named") == "info"
    );

    // A body that carries no level at all, alongside a query string that
    // does. Every case above has the level in the body whenever the body is
    // non-empty, so the merge could have dropped the query string entirely
    // and they would all still pass.
    check!(requested(Some("log_level=debug"), "other=1").expect("a level is named") == "debug");

    // An empty query string alongside a body is not a source.
    check!(requested(Some(""), "log_level=error").expect("a level is named") == "error");

    // Neither source, and an unrecognised level, are refused distinctly.
    check!(let Err(HttpQueryError::MissingQueryParameter("log_level")) = requested(None, ""));
    check!(
        let Err(HttpQueryError::InvalidQueryParameter { name: "log_level", .. }) =
            requested(Some("log_level=verbose"), "")
    );
    check!(
        requested(Some("log_level=verbose"), "")
            .expect_err("an unrecognised level is refused")
            .to_string()
            .contains("verbose")
    );
}
