use super::*;

/// `parse_patterns_params` requires query, start and end, defaults only the
/// step, and takes the LAST value of a repeated parameter. That last part
/// is the opposite of `parse_series_params`, which keeps the first -- the
/// two live in the same file and differ only by an `is_none()` guard, so
/// each is pinned with the contrast stated.
///
/// Each required parameter names ITSELF when missing, so a client sending
/// two of the three is told which one it forgot rather than a generic
/// failure.
#[test]
pub(crate) fn patterns_params_require_three_and_take_the_last_of_each() {
    let parse = |query: &str| super::super::prelude::parse_patterns_params(Some(query));

    let params = parse("query=up&start=100&end=200").expect("all three present");
    check!(params.query == "up");
    check!(params.start == 100);
    check!(params.end == 200);
    check!(
        params.step == 1_000_000_000,
        "the step defaults to a second"
    );

    // A repeated parameter keeps the LAST value.
    let params = parse("query=a&query=b&start=100&end=200").expect("parses");
    check!(params.query == "b", "the last query, unlike series params");
    let params = parse("query=up&start=100&start=300&end=200").expect("parses");
    check!(params.start == 300, "and the last start");

    // An explicit step overrides the default.
    let params = parse("query=up&start=100&end=200&step=5s").expect("parses");
    check!(params.step == 5_000_000_000);

    // Each required parameter names itself when absent.
    check!(matches!(
        parse("start=100&end=200"),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));
    check!(matches!(
        parse("query=up&end=200"),
        Err(HttpQueryError::MissingQueryParameter("start"))
    ));
    check!(matches!(
        parse("query=up&start=100"),
        Err(HttpQueryError::MissingQueryParameter("end"))
    ));
    check!(matches!(
        super::super::prelude::parse_patterns_params(None),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));

    // A malformed bound is refused rather than dropped.
    check!(parse("query=up&start=nonsense&end=200").is_err());
}
