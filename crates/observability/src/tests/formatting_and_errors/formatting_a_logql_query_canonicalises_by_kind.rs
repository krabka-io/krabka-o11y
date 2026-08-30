use super::*;

/// `format_logql_query` returns the canonical spelling of a query, and
/// what "canonical" means differs by the kind of query it is: a stream
/// selector round-trips, a scalar expression is folded to its value, and a
/// vector literal gains an explicit float. Nothing tested any of it --
/// returning an empty string passed the whole suite.
#[test]
pub(crate) fn formatting_a_logql_query_canonicalises_by_kind() {
    let format =
        |query: &str| super::super::prelude::format_logql_query(query).map_err(|error| error.to_string());

    // Stream selectors and pipelines come back as they went in.
    check!(format(r#"{app="web"}"#).unwrap() == r#"{app="web"}"#);
    check!(
        format(r#"{app="web"} |= "boom""#).unwrap() == r#"{app="web"} |= "boom""#,
        "a line filter survives"
    );
    check!(
        format(r#"rate({app="web"}[5m])"#).unwrap() == r#"rate({app="web"}[5m])"#,
        "and a range aggregation"
    );
    check!(format(r#"sum(rate({app="web"}[5m]))"#).unwrap() == r#"sum(rate({app="web"}[5m]))"#);

    // Surrounding whitespace is not part of the query. A stream selector
    // is rebuilt from its parse, so it would come back canonical however
    // it was spaced; the second case is the one that proves trimming,
    // because it is returned as written and only the trim can remove the
    // spaces around it.
    check!(format(r#"  {app="web"}  "#).unwrap() == r#"{app="web"}"#);
    check!(
        format(r#"  sum by (app) (rate({app="web"}[5m])) / 2  "#).unwrap()
            == r#"sum by (app) (rate({app="web"}[5m])) / 2"#,
        "returned as written, less the surrounding space"
    );

    // A comparison gains explicit parentheses, and label_replace loses the
    // spaces between its arguments. Both are reprintings rather than
    // pass-throughs, so neither can be reached by the trim above.
    check!(
        format(r#"count_over_time({app="web"}[5m]) > 1"#).unwrap()
            == r#"(count_over_time({app="web"}[5m]) > 1)"#
    );
    check!(
        format(r#"label_replace(rate({app="web"}[5m]), "a", "b", "c", "d")"#).unwrap()
            == r#"label_replace(rate({app="web"}[5m]),"a","b","c","d")"#
    );

    // A scalar expression is evaluated rather than echoed, which is a
    // different contract from every case above.
    check!(format("1 + 1").unwrap() == "2", "folded, not reprinted");

    // A vector literal is normalised to an explicit float.
    check!(format("vector(1)").unwrap() == "vector(1.000000)");

    // These two reach the fallback that returns a query as written: the
    // dedicated formatter for their shape declines, and only the scalar
    // comparison and the vector-expression parsers below it accept them.
    // Everything above is a reprint, so a pass-through is the signature of
    // having got that far.
    for query in [r#"sum(rate({app="web"}[5m])) > 5"#, "vector(1) + 2"] {
        check!(format(query).unwrap() == query, "{query}");
    }

    // What is not a query at all is an error naming where it gave up,
    // rather than an empty string or the input echoed back.
    let error = format("").unwrap_err();
    check!(error.contains("byte 0"), "got: {error}");
    let error = format("not a query at all").unwrap_err();
    check!(error.contains("byte 0"), "got: {error}");
    let error = format("{").unwrap_err();
    check!(
        error.contains("label name"),
        "a partial selector names what it wanted: {error}"
    );
}
