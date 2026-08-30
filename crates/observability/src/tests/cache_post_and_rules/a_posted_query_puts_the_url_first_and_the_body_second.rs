use super::*;

/// `post_query_params` merges a URL query with a form body. It has a
/// near-twin, `post_query_params_body_first`, which differs only in which
/// side leads the result -- so every case here asserts the order, not just
/// the contents. A test that checked membership alone would pass against
/// either function and distinguish neither.
#[test]
pub(crate) fn a_posted_query_puts_the_url_first_and_the_body_second() {
    let merge = |raw: Option<&str>, body: &str| {
        super::super::prelude::post_query_params(raw, &Bytes::from(body.to_owned()))
            .expect("valid body")
    };
    let body_first = |raw: Option<&str>, body: &str| {
        super::super::prelude::post_query_params_body_first(raw, &Bytes::from(body.to_owned()))
            .expect("valid body")
    };

    // Both sides present: the order is the whole difference between the
    // two functions.
    check!(merge(Some("a=1"), "b=2") == "a=1&b=2");
    check!(body_first(Some("a=1"), "b=2") == "b=2&a=1");

    // One side only, where the two agree.
    check!(merge(Some("a=1"), "") == "a=1");
    check!(merge(None, "b=2") == "b=2");
    check!(merge(None, "") == "");

    // An empty URL query is treated as absent rather than concatenated,
    // which would otherwise leave a leading separator.
    check!(merge(Some(""), "b=2") == "b=2");
    check!(merge(Some(""), "") == "");
    check!(body_first(Some(""), "b=2") == "b=2");
}
