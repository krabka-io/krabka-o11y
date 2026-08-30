use super::*;

/// `parse_series_params` treats its parameters asymmetrically, and the
/// asymmetry is deliberate: matchers ACCUMULATE, because a series request
/// may carry several, while the time bounds are FIRST-WINS, because a
/// second one is a client mistake rather than an addition. A fixture
/// sending each parameter once cannot tell the two rules apart.
#[test]
pub(crate) fn series_params_accumulate_matchers_but_keep_the_first_time_bound() {
    let parse = |query: &str| super::super::prelude::parse_series_params(Some(query));

    // Both spellings of a matcher, accumulating in the order sent.
    let params = parse("match[]=a&match[]=b").expect("matchers parse");
    check!(params.matchers == vec!["a".to_string(), "b".to_string()]);
    let params = parse("query=a&query=b").expect("matchers parse");
    check!(params.matchers == vec!["a".to_string(), "b".to_string()]);
    // And the two spellings share one list.
    let params = parse("match[]=a&query=b").expect("matchers parse");
    check!(params.matchers == vec!["a".to_string(), "b".to_string()]);

    // The percent-encoded spelling of `match[]` is accepted too.
    let params = parse("match%5B%5D=a").expect("matchers parse");
    check!(params.matchers == vec!["a".to_string()]);

    // Time bounds keep the FIRST value, not the last. A bare integer is
    // read as nanoseconds directly rather than as seconds.
    let params = parse("start=100&start=200").expect("bounds parse");
    check!(params.start == Some(100), "the first bound, in nanoseconds");
    let params = parse("end=100&end=200").expect("bounds parse");
    check!(params.end == Some(100));
    // A decimal is seconds, and RFC3339 is accepted too -- three
    // spellings reaching one field.
    check!(parse("start=1.5").expect("decimal seconds").start == Some(1_500_000_000));
    check!(parse("start=1970-01-01T00:00:01Z").expect("rfc3339").start == Some(1_000_000_000));

    // Absent parameters stay absent rather than defaulting.
    let params = parse("query=a").expect("a query alone parses");
    check!(params.start.is_none());
    check!(params.end.is_none());
    check!(params.since.is_none());

    // No query string at all is not an error.
    let params = super::super::prelude::parse_series_params(None).expect("no query is valid");
    check!(params.matchers.is_empty());

    // Unknown parameters are ignored rather than refused.
    check!(
        parse("nonsense=1")
            .expect("unknown keys are ignored")
            .matchers
            .is_empty()
    );

    // A malformed bound IS refused, since silently dropping it would run
    // the query over a window the client did not ask for.
    check!(parse("start=nonsense").is_err());
}
