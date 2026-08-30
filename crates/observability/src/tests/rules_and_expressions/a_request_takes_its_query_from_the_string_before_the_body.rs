use super::*;

/// `request_query_or_form_body` picks ONE source rather than merging them,
/// and the query string wins. That is the opposite of `log_level_post`,
/// which merges and lets the body win -- the two are pinned separately
/// because a reader who knows one would guess the other wrong.
///
/// An empty source is not a source: an empty query string falls through to
/// the body rather than being returned as an empty query, which would
/// produce a "missing parameter" error naming the wrong cause.
#[test]
pub(crate) fn a_request_takes_its_query_from_the_string_before_the_body() {
    let take = |raw_query: Option<&str>, body: &[u8]| {
        super::super::prelude::request_query_or_form_body(
            raw_query,
            &axum::body::Bytes::from(body.to_vec()),
        )
    };

    // The query string wins when both carry something.
    check!(take(Some("query=a"), b"query=b").ok().as_deref() == Some("query=a"));
    // Either alone.
    check!(take(Some("query=a"), b"").ok().as_deref() == Some("query=a"));
    check!(take(None, b"query=b").ok().as_deref() == Some("query=b"));

    // An empty query string is not a source, so the body is used.
    check!(take(Some(""), b"query=b").ok().as_deref() == Some("query=b"));

    // Neither source is a missing-parameter error, distinct from a
    // malformed one.
    check!(matches!(
        take(None, b""),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));
    check!(matches!(
        take(Some(""), b""),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));

    // A body that is not UTF-8 is refused rather than read lossily: a
    // replacement character in a matcher would change what was queried.
    check!(matches!(
        take(None, &[0xff, 0xfe]),
        Err(HttpQueryError::InvalidPercentEncoding)
    ));
    // But only when the body is the source being used.
    check!(
        take(Some("query=a"), &[0xff, 0xfe]).ok().as_deref() == Some("query=a"),
        "an unused body is not validated"
    );
}
