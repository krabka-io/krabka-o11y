use krabka_units::convert::TimeExt as _;

use super::prelude::{HttpQueryError, Time, check};
/// `read_loki_rule_tenants` treats a MISSING rules file as no rules, and
/// every other I/O failure as an error. That distinction is the point: a
/// store that has never had a rule written to it has no file, and starting
/// up must not fail because of it -- while a file that exists and cannot
/// be read is a real problem the operator needs told about.
///
/// Malformed JSON is likewise an error rather than an empty result:
/// silently discarding every rule in a corrupt file would stop alerting
/// without saying so.
#[test]
pub(crate) fn missing_loki_rules_are_empty_but_unreadable_ones_are_an_error() {
    let dir = tempfile::tempdir().expect("a temp dir");

    // Absent: no rules, no error.
    let absent = dir.path().join("absent.json");
    let tenants =
        super::prelude::read_loki_rule_tenants(&absent).expect("an absent file is not an error");
    check!(tenants.is_empty());

    // Present and valid: the rules come back.
    let valid = dir.path().join("valid.json");
    std::fs::write(
        &valid,
        r#"{"tenant-a":{"namespace":{"group":{"rules":[]}}}}"#,
    )
    .expect("the fixture writes");
    let tenants = super::prelude::read_loki_rule_tenants(&valid).expect("valid json parses");
    check!(tenants.len() == 1);
    check!(tenants.contains_key("tenant-a"));

    // Present and empty-but-valid.
    let empty = dir.path().join("empty.json");
    std::fs::write(&empty, "{}").expect("the fixture writes");
    check!(
        super::prelude::read_loki_rule_tenants(&empty)
            .expect("an empty object parses")
            .is_empty()
    );

    // Present and malformed: an error, NOT an empty set. Returning empty
    // here would silently stop alerting on every rule in the file.
    let malformed = dir.path().join("malformed.json");
    std::fs::write(&malformed, "{not json").expect("the fixture writes");
    check!(matches!(
        super::prelude::read_loki_rule_tenants(&malformed),
        Err(super::prelude::LokiRuleStoreError::Json { .. })
    ));

    // A directory where a file was expected is an I/O error, which is how
    // a non-NotFound failure is reached without special privileges.
    check!(matches!(
        super::prelude::read_loki_rule_tenants(dir.path()),
        Err(super::prelude::LokiRuleStoreError::Io { .. })
    ));
}

/// `scalar_vector_expression_result` evaluates the scalar-and-vector
/// sub-language: arithmetic over numbers, and `vector(...)` producing a
/// series. Two things about it are easy to get wrong and are pinned here.
///
/// First, whitespace is stripped BEFORE parsing rather than skipped during
/// it, so "1 + 1" and "1+1" are the same expression -- and so, less
/// happily, are "1 1" and "11". Second, the parser
/// must be FINISHED: "1+1x" is refused rather than evaluated as "1+1" with
/// the tail ignored, which would silently accept a typo as a valid query.
#[test]
pub(crate) fn a_scalar_vector_expression_must_consume_its_whole_query() {
    use super::prelude::ScalarVectorExpressionResult;

    let result = super::prelude::scalar_vector_expression_result;
    let scalar = |query: &str| match result(query) {
        Some(ScalarVectorExpressionResult::Scalar { sample }) => Some(sample),
        _ => None,
    };

    // Plain arithmetic, with and without spaces.
    check!(scalar("1").as_deref() == Some("1"));
    check!(scalar("1+1").as_deref() == Some("2"));
    check!(
        scalar("1 + 1").as_deref() == Some("2"),
        "whitespace is stripped first"
    );
    check!(scalar("  2 * 3  ").as_deref() == Some("6"));
    check!(
        scalar("(1+2)*3").as_deref() == Some("9"),
        "parentheses group"
    );

    // A vector literal is the other result shape.
    check!(matches!(
        result("vector(1)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(
        matches!(
            result("vector( 1 )"),
            Some(ScalarVectorExpressionResult::Vector { .. }),
        ),
        "whitespace inside the call too"
    );

    // Trailing junk is refused rather than ignored. This is the case that
    // the `is_finished` check exists for: without it "1+1x" evaluates to 2
    // and a typo becomes a valid query.
    check!(result("1+1x").is_none());
    check!(result("vector(1)x").is_none());
    // But "1 1" is not junk -- stripping whitespace FIRST makes it the
    // single number eleven. That follows from the strip being a rewrite of
    // the input rather than a skip during parsing, and it is pinned
    // because it is surprising, not because it is desirable.
    check!(scalar("1 1").as_deref() == Some("11"));

    // A set operator needs a vector on BOTH sides. Each of the two counts
    // is a strict increase over the terms seen before that side was
    // parsed, and "at least as many" is trivially true -- so a side with
    // no vector at all is the only thing that separates them.
    check!(matches!(
        result("vector(1) and vector(2)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(result("1 and vector(1)").is_none(), "no vector on the left");
    check!(result("vector(1) and 1").is_none(), "none on the right");
    check!(result("1 and 1").is_none(), "none on either side");

    // A comparison carrying `on(...)`/`ignoring(...)` needs a vector on
    // both sides too. Without a modifier the same comparison is fine, so
    // the modifier is what turns the requirement on.
    check!(
        result("vector(1) > 0").is_some(),
        "no modifier, no requirement"
    );
    check!(matches!(
        result("vector(1) > on() vector(2)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(
        result("1 > on() vector(1)").is_none(),
        "a modifier with no vector on the left"
    );
    check!(
        result("vector(1) > ignoring() 1").is_none(),
        "and none on the right"
    );

    // An escape inside a string literal is decoded, and the parser advances
    // PAST it. Every other string here is escape-free, where advancing the
    // wrong way would go unnoticed.
    let replaced = result(r#"label_replace(vector(1),"dst","a\nb","src","(.*)")"#);
    let Some(ScalarVectorExpressionResult::Vector { metric, .. }) = replaced else {
        panic!("expected a vector result");
    };
    check!(
        metric.get("dst").map(String::as_str) == Some("a\nb"),
        "got {metric:?}"
    );

    // Not this sub-language at all.
    check!(result("up").is_none());
    check!(result("").is_none());
    check!(result("+").is_none());
    check!(result("(1").is_none(), "an unclosed group is not finished");
}

/// `format_loki_query_length` always writes all three units, including the
/// zero ones -- "0h5m0s" rather than "5m". That is the opposite of
/// `format_loki_duration_ns`, which skips empty units, and the two are
/// pinned separately because the difference is deliberate: this one is a
/// fixed-shape field a client parses positionally.
#[test]
pub(crate) fn a_loki_query_length_always_writes_all_three_units() {
    let format = |seconds: i64| super::prelude::format_loki_query_length(Time::from_nanos(seconds));
    let secs = 1_000_000_000_i64;

    check!(format(0) == "0h0m0s", "every unit, even at zero");
    check!(format(5 * secs) == "0h0m5s");
    check!(format(300 * secs) == "0h5m0s", "zero seconds still written");
    check!(format(3_600 * secs) == "1h0m0s");
    check!(format(3_661 * secs) == "1h1m1s");
    check!(format(7_322 * secs) == "2h2m2s");

    // Hours accumulate rather than rolling into a larger unit.
    check!(format(100 * 3_600 * secs) == "100h0m0s");

    // Sub-second precision is dropped, not rounded up.
    check!(format(secs - 1) == "0h0m0s");

    // A negative range is clamped to zero rather than writing minus signs
    // into a field a client parses positionally.
    check!(format(-secs) == "0h0m0s");
}

/// `validate_loki_interval` refuses a negative step and accepts everything
/// else, including zero and an absent one. Zero is the boundary that
/// separates `< 0` from `<= 0`, and an absent interval is not the same as
/// a zero one -- absent means the caller did not ask.
#[test]
pub(crate) fn a_loki_interval_is_refused_only_when_negative() {
    let validate = super::prelude::validate_loki_interval;

    check!(validate(None).is_ok(), "an absent interval is not an error");
    check!(validate(Some(0)).is_ok(), "and neither is zero");
    check!(validate(Some(1)).is_ok());
    check!(validate(Some(i64::MAX)).is_ok());
    check!(matches!(
        validate(Some(-1)),
        Err(HttpQueryError::InvalidInterval)
    ));
    check!(validate(Some(i64::MIN)).is_err());
}

/// `normalize_loki_vector_sample_timestamps_to_seconds` rewrites each
/// instant sample's timestamp from nanoseconds to seconds in place. It
/// accepts the timestamp as a JSON number OR a string, since both spellings
/// reach it, and it writes back a whole number when the nanos divide
/// exactly and a float otherwise -- a client parsing "1700000000" as an
/// integer must not be handed "1700000000.0".
#[test]
pub(crate) fn loki_vector_timestamps_are_rewritten_from_nanos_to_seconds() {
    let normalize = |timestamp: serde_json::Value| {
        let mut value = serde_json::json!({
            "data": {"result": [{"metric": {}, "value": [timestamp, "1"]}]}
        });
        super::prelude::normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        value["data"]["result"][0]["value"][0].clone()
    };

    // An exact second becomes an integer, in both spellings.
    check!(normalize(serde_json::json!(1_700_000_000_000_000_000_u64)) == 1_700_000_000);
    check!(normalize(serde_json::json!("1700000000000000000")) == 1_700_000_000);

    // A fractional second becomes a float rather than being truncated.
    check!(normalize(serde_json::json!(1_500_000_000_u64)) == 1.5);
    check!(normalize(serde_json::json!("1500000000")) == 1.5);

    check!(normalize(serde_json::json!(0_u64)) == 0);

    // A timestamp that is neither a number nor a string is left alone
    // rather than replaced with a default.
    check!(normalize(serde_json::json!(true)) == true);

    // A response with no result array is left untouched.
    let mut empty = serde_json::json!({"status": "success"});
    let before = empty.clone();
    super::prelude::normalize_loki_vector_sample_timestamps_to_seconds(&mut empty);
    check!(empty == before);
}

/// `parse_metric_vector_comparison_expression` recognises a comparison
/// between a metric query and a `vector(...)` literal, and records WHICH
/// side the literal was on -- the two are not interchangeable, since
/// `up > vector(1)` and `vector(1) > up` select opposite samples.
///
/// Exactly one side must be a vector literal. Two of them, or none, is not
/// this kind of expression and is refused rather than guessed at, so both
/// rejections are checked as well as both acceptances.
#[test]
pub(crate) fn a_vector_comparison_records_which_side_the_literal_was_on() {
    use krabka_logql::ComparisonOp;

    let parse = super::prelude::parse_metric_vector_comparison_expression;

    let right = parse("up > vector(1)").expect("a vector on the right");
    check!(right.metric_query == "up");
    check!(right.vector_query == "vector(1)");
    check!(!right.vector_on_left);
    check!(right.op == ComparisonOp::Greater);
    check!(!right.bool_modifier);

    let left = parse("vector(1) > up").expect("a vector on the left");
    check!(left.metric_query == "up", "the metric is still the metric");
    check!(left.vector_query == "vector(1)");
    check!(left.vector_on_left, "but the side is recorded");
    check!(left.op == ComparisonOp::Greater);

    // The `bool` modifier is stripped from the right and remembered.
    let modified = parse("up > bool vector(1)").expect("bool is allowed");
    check!(modified.bool_modifier);
    check!(
        modified.vector_query == "vector(1)",
        "bool is not part of the query"
    );
    check!(modified.metric_query == "up");

    // Every comparison operator reaches the expression.
    for (query, op) in [
        ("up == vector(1)", ComparisonOp::Equal),
        ("up != vector(1)", ComparisonOp::NotEqual),
        ("up < vector(1)", ComparisonOp::Less),
        ("up <= vector(1)", ComparisonOp::LessEqual),
        ("up >= vector(1)", ComparisonOp::GreaterEqual),
    ] {
        check!(parse(query).expect("parses").op == op, "{query}");
    }

    // Two vectors, or none, is not this kind of expression.
    check!(parse("vector(1) > vector(2)").is_none(), "two literals");
    check!(parse("up > down").is_none(), "no literal");
    check!(parse("up").is_none(), "no comparison at all");
    check!(parse("").is_none());
}

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
        super::prelude::request_query_or_form_body(
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

/// `split_query_param_pairs` breaks a query string on `&` only when a
/// KNOWN key follows it. That is not the usual rule, and it exists because
/// a `LogQL` matcher can contain an ampersand -- splitting on every one
/// would cut a query in half and leave both halves unparseable.
#[test]
pub(crate) fn a_query_string_splits_only_before_a_known_key() {
    fn split(query: &str) -> Vec<&str> {
        super::prelude::split_query_param_pairs(query, &["query", "start", "end"])
    }

    check!(split("query=up") == vec!["query=up"]);
    check!(split("query=up&start=1") == vec!["query=up", "start=1"]);
    check!(split("query=up&start=1&end=2") == vec!["query=up", "start=1", "end=2"]);

    // An ampersand inside a value is kept, because what follows it is not
    // a known key. This is the case the whole function exists for.
    check!(
        split(r#"query={app="a&b"}&start=1"#) == vec![r#"query={app="a&b"}"#, "start=1"],
        "the matcher keeps its ampersand"
    );
    check!(
        split("query=a&b=c") == vec!["query=a&b=c"],
        "b is not a known key"
    );

    // A known key needs its `=` to count as one: "&start" alone is text.
    check!(split("query=a&start") == vec!["query=a&start"]);
    check!(
        split("query=a&startle=1") == vec!["query=a&startle=1"],
        "not a prefix match"
    );

    // Empty segments are dropped rather than yielded as empty strings.
    check!(split("") == Vec::<&str>::new());
    check!(split("&query=a") == vec!["query=a"]);
    // A trailing `&` is KEPT, since nothing follows it to be a known key.
    // The rule is about what comes after the ampersand, not about the
    // ampersand itself.
    check!(split("query=a&") == vec!["query=a&"]);
}

/// `parse_series_params` treats its parameters asymmetrically, and the
/// asymmetry is deliberate: matchers ACCUMULATE, because a series request
/// may carry several, while the time bounds are FIRST-WINS, because a
/// second one is a client mistake rather than an addition. A fixture
/// sending each parameter once cannot tell the two rules apart.
#[test]
pub(crate) fn series_params_accumulate_matchers_but_keep_the_first_time_bound() {
    let parse = |query: &str| super::prelude::parse_series_params(Some(query));

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
    let params = super::prelude::parse_series_params(None).expect("no query is valid");
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
