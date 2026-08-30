use super::prelude::{BlockIndex, LabelIndex, Labels, QuerierState, SeriesParams, check};
/// `format_loki_offset_duration_ns` spells a duration the way `Loki` does,
/// picking the largest unit that fits. Each `>=` is the boundary between
/// two units, so each is checked exactly at its own threshold and one
/// step below it -- a `<` there sends the value to the next unit down.
#[test]
pub(crate) fn a_loki_offset_duration_picks_the_largest_unit_that_fits() {
    let format = super::prelude::format_loki_offset_duration_ns;

    // Zero is a duration, not an absence. A `<= 0` guard would lose it.
    check!(format(0) == Some("0s".to_string()));
    // Negative is an absence, which is what separates `< 0` from `== 0`.
    check!(format(-1).is_none());
    check!(format(-3_600_000_000_000).is_none());

    // At and just below the seconds boundary.
    check!(format(1_000_000_000) == Some("1s".to_string()));
    check!(format(1_500_000_000) == Some("1.5s".to_string()));
    check!(format(999_999_999) == Some("999.999999ms".to_string()));

    // At and just below the milliseconds boundary.
    check!(format(1_000_000) == Some("1ms".to_string()));
    check!(format(999_999) == Some("999.999\u{00b5}s".to_string()));

    // At and just below the microseconds boundary.
    check!(format(1_000) == Some("1\u{00b5}s".to_string()));
    check!(format(999) == Some("999ns".to_string()));

    // Larger units compose rather than replacing one another.
    check!(format(3_600_000_000_000) == Some("1h0m0s".to_string()));
    check!(format(90_000_000_000) == Some("1m30s".to_string()));
}

/// `apply_loki_stream_limit` spends one budget across several streams,
/// truncating the last one that fits. The budget only visibly decrements
/// when an earlier stream takes part of it and a later stream needs the
/// rest -- with a single stream, any arithmetic on the remainder looks
/// alike.
#[test]
pub(crate) fn a_loki_stream_limit_is_spent_across_streams_in_order() {
    let streams = |counts: &[usize]| {
        serde_json::json!({
            "data": {
                "resultType": "streams",
                "result": counts
                    .iter()
                    .map(|count| serde_json::json!({
                        "stream": {"app": "a"},
                        "values": (0..*count)
                            .map(|i| serde_json::json!([i.to_string(), "line"]))
                            .collect::<Vec<_>>(),
                    }))
                    .collect::<Vec<_>>(),
            }
        })
    };
    let kept = |value: &serde_json::Value| {
        value
            .pointer("/data/result")
            .and_then(serde_json::Value::as_array)
            .expect("the result is an array")
            .iter()
            .map(|stream| {
                stream
                    .get("values")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len)
            })
            .collect::<Vec<_>>()
    };

    // The first stream takes 2 of the 5, leaving 3 for the second.
    // Adding instead would leave 7, and dividing would leave 2.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[2, 10]),
            Some(5)
        )) == vec![2, 3]
    );

    // A stream that exhausts the budget empties every stream after it,
    // and emptied streams are dropped entirely.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[5, 10]),
            Some(5)
        )) == vec![5]
    );

    // Under budget, nothing is touched.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[2, 2]),
            Some(5)
        )) == vec![2, 2]
    );

    // No limit means no truncation, and a non-streams result is left alone.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[9]),
            None
        )) == vec![9]
    );
}

/// The two `LogQL` token namers turn a parser's own wording into the token
/// names `Loki`'s clients expect. Each named arm falls through to a generic
/// rewrite when deleted, so every one is pinned to its own answer.
#[test]
pub(crate) fn logql_parse_errors_name_the_tokens_loki_clients_expect() {
    let expected = super::prelude::expected_logql_token;
    let unexpected = super::prelude::unexpected_logql_token;

    check!(expected("expected '\"'") == "STRING");
    check!(expected("expected closing quote") == "STRING");
    check!(expected("expected label matcher operator") == "ASSIGN, EQ, NEQ, RE, NRE");
    check!(expected("expected label name") == "IDENTIFIER");
    check!(expected("expected end of query") == "$end");

    // Anything else keeps its wording with the lead-in stripped, which is
    // what a deleted arm above would fall through to.
    check!(expected("expected a pipeline stage") == "a pipeline stage");
    check!(expected("something else entirely") == "something else entirely");

    // An underscore starts an identifier just as a letter does, which is
    // the case separating `==` from `!=` in that test.
    check!(unexpected("_foo", 0) == "IDENTIFIER");
    check!(unexpected("foo", 0) == "IDENTIFIER");
    check!(
        unexpected("{app=\"a\"}", 0) == "{",
        "punctuation names itself"
    );
    check!(
        unexpected("1", 0) == "1",
        "and a digit is not an identifier"
    );
    check!(unexpected("", 0) == "$end");
    check!(
        unexpected("abc", 99) == "$end",
        "a position past the end is the end"
    );
}

/// `hex_value` maps a hex digit to its value across three ranges. Every
/// range boundary is checked together with the character immediately
/// outside it, since a range widened or narrowed by one is invisible from
/// the middle -- and the two letter ranges must not be confused, because
/// their offsets differ by the distance between the cases.
#[test]
pub(crate) fn hex_digits_map_across_all_three_ranges_and_nothing_else() {
    let value = super::prelude::hex_value;

    check!(value(b'0') == Some(0), "the low edge of the digits");
    check!(value(b'9') == Some(9), "and the high edge");
    check!(value(b'5') == Some(5));
    check!(value(b'a') == Some(10), "lower-case a continues from nine");
    check!(value(b'f') == Some(15));
    check!(value(b'A') == Some(10), "upper-case is the same value");
    check!(value(b'F') == Some(15));

    // One character outside each range, on both sides.
    check!(value(b'/') == None, "just below '0'");
    check!(value(b':') == None, "just above '9'");
    check!(value(b'`') == None, "just below 'a'");
    check!(value(b'g') == None, "just above 'f'");
    check!(value(b'@') == None, "just below 'A'");
    check!(value(b'G') == None, "just above 'F'");

    // The gap between the two letter ranges is not a range.
    check!(value(b'Z') == None);
    check!(value(b' ') == None);
}

/// `parse_decimal_seconds_timestamp` reads seconds with a fractional part
/// into whole nanoseconds. The fraction is positional -- the first digit
/// is tenths, not units -- so a scale applied the wrong way round is the
/// mistake worth catching, and it only shows on a fraction shorter than
/// nine digits.
#[test]
pub(crate) fn decimal_second_timestamps_scale_their_fraction_by_position() {
    let parse = super::prelude::parse_decimal_seconds_timestamp;

    check!(parse("0.0") == Some(0));
    check!(parse("1.0") == Some(1_000_000_000));
    check!(
        parse("1.5") == Some(1_500_000_000),
        "one digit is tenths, not units"
    );
    check!(parse("0.5") == Some(500_000_000));
    check!(
        parse("0.05") == Some(50_000_000),
        "the second digit is hundredths"
    );
    check!(
        parse("0.000000001") == Some(1),
        "nine digits reach nanoseconds"
    );

    // Past nine digits the rest is dropped rather than overflowing the
    // scale into zero or below.
    check!(
        parse("0.0000000019") == Some(1),
        "the tenth digit is ignored"
    );

    // Signs, on both sides of zero.
    check!(parse("-1.5") == Some(-1_500_000_000));
    check!(
        parse("+1.5") == Some(1_500_000_000),
        "an explicit plus is allowed"
    );
    check!(parse("-0.0") == Some(0));

    // A missing part on either side of the point is still a number.
    check!(parse("1.") == Some(1_000_000_000), "no fraction");
    check!(parse(".5") == Some(500_000_000), "no whole part");

    // What is not a decimal at all.
    check!(parse("1") == None, "a point is required");
    check!(parse(".") == None, "and digits on one side of it");
    check!(parse("") == None);
    check!(parse("a.b") == None);
    check!(parse("1.5x") == None, "trailing text is not a fraction");
}

/// `scalar_literal_len` reports how many bytes at the front of `input`
/// form a number, so the caller can resume after it. It is a scanner, not
/// a parser: it must stop at the first byte that cannot extend the
/// literal, and refuse anything that is not one.
#[test]
pub(crate) fn a_scalar_literal_ends_where_the_number_does() {
    let len = super::prelude::scalar_literal_len;

    check!(len("1") == Some(1));
    check!(len("1234") == Some(4));
    check!(len("+1") == Some(2), "a leading sign counts");
    check!(len("-1") == Some(2));

    // A fraction may sit on either side of the point.
    check!(len("1.5") == Some(3));
    check!(len(".5") == Some(2), "no whole part is still a number");
    check!(len("1.") == Some(2), "a trailing point ends the literal");
    check!(len("+.5") == Some(3));

    // An exponent takes an optional sign and needs at least one digit.
    check!(len("1e5") == Some(3));
    check!(len("1e+5") == Some(4));
    check!(len("1e-5") == Some(4));
    check!(len("1.5e10") == Some(6));
    check!(len("1E5") == Some(3), "an exponent may be upper case");
    check!(len("1E-5") == Some(4));
    check!(
        len("1e") == None,
        "an exponent with no digits is not a number"
    );
    check!(len("1e+") == None);

    // Nothing that is not a number.
    check!(len("") == None);
    check!(len(".") == None, "a bare point has no digits either side");
    check!(len("+") == None);
    check!(len("abc") == None);

    // The scan stops at the first byte it cannot use, rather than
    // rejecting the whole input.
    check!(len("1abc") == Some(1));
    check!(len("1.5]") == Some(3));
    check!(len("1e5x") == Some(3));
}

/// `metadata_label_sets` lists the distinct label sets a tenant has,
/// filtered by the request's matchers and hiding the labels that are
/// internal. Replacing its body with an empty list passed the whole suite
/// before this test, so every part of it is pinned here.
#[tokio::test]
pub(crate) async fn metadata_label_sets_are_distinct_filtered_and_stripped() {
    async fn sets(state: &QuerierState, matchers: Vec<String>) -> Vec<Labels> {
        let params = SeriesParams {
            matchers,
            start: None,
            end: None,
            since: None,
        };
        super::prelude::metadata_label_sets(state, "t", &params)
            .await
            .expect("readable")
    }
    let mut label_index = LabelIndex::default();
    let labels = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Labels>()
    };
    // Two series for one tenant plus one for another, and a fourth that
    // differs from the first only by an internal label -- so it collapses
    // onto it once that label is hidden.
    label_index.insert_series("t", labels(&[("app", "web"), ("env", "prod")]));
    label_index.insert_series("t", labels(&[("app", "api"), ("env", "prod")]));
    label_index.insert_series(
        "t",
        labels(&[("app", "web"), ("env", "prod"), ("detected_level", "warn")]),
    );
    label_index.insert_series("other", labels(&[("app", "elsewhere")]));

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), label_index, BlockIndex::default());

    // Unfiltered: the two distinct visible sets, with the third collapsed
    // onto the first because its only difference is hidden.
    let all = sets(&state, Vec::new()).await;
    check!(all.len() == 2, "got {all:?}");
    check!(
        all.iter().all(|set| set.get("detected_level").is_none()),
        "the internal label is stripped, not reported"
    );

    // Another tenant's series are not this tenant's.
    check!(
        all.iter()
            .all(|set| set.get("app").map(String::as_str) != Some("elsewhere")),
        "tenant isolation"
    );

    // A matcher narrows the result rather than being ignored.
    let web = sets(&state, vec![r#"{app="web"}"#.to_string()]).await;
    check!(web.len() == 1, "got {web:?}");
    check!(web[0].get("app").map(String::as_str) == Some("web"));

    let none = sets(&state, vec![r#"{app="absent"}"#.to_string()]).await;
    check!(
        none.is_empty(),
        "a matcher that matches nothing returns nothing"
    );
}

/// The hot tail is bounded twice over: to the requesting tenant, and to
/// the requested window at both edges inclusively. Nothing had ever read
/// it through this endpoint, so a record from another tenant, one before
/// the window and one after it were all free to be reported, and the two
/// edges were free to exclude a record sitting exactly on them.
#[tokio::test]
pub(crate) async fn metadata_label_sets_bound_the_hot_tail_to_the_window_and_the_tenant() {
    // A hot tail is allowed to answer a range query with a *superset* --
    // the trait says so, because a coarse time index returns whole buckets
    // -- and the caller re-applies the exact bound. This one returns
    // everything, which is the widest superset there is.
    struct CoarseHotTail(Vec<super::prelude::WalLogRecord>);
    impl super::prelude::LogHotTail for CoarseHotTail {
        fn records(&self) -> Vec<super::prelude::WalLogRecord> {
            self.0.clone()
        }
        fn records_in_range(
            &self,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Vec<super::prelude::WalLogRecord> {
            self.0.clone()
        }
    }

    let record = |tenant: &str, app: &str, timestamp_ns: i64| super::prelude::WalLogRecord {
        tenant: tenant.to_string(),
        labels: [("app".to_string(), app.to_string())]
            .into_iter()
            .collect::<Labels>(),
        timestamp_ns,
        line: "line".to_string(),
        structured_metadata: std::collections::BTreeMap::new(),
        position: None,
    };
    let sink = CoarseHotTail(vec![
        record("t", "on_the_start", 100),
        record("t", "inside", 150),
        record("t", "on_the_end", 200),
        record("t", "before", 99),
        record("t", "after", 201),
        record("other", "foreign", 150),
    ]);

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink, 0);

    let params = SeriesParams {
        matchers: Vec::new(),
        start: Some(100),
        end: Some(200),
        since: None,
    };
    let sets = super::prelude::metadata_label_sets(&state, "t", &params)
        .await
        .expect("readable");

    let mut apps = sets
        .iter()
        .filter_map(|set| set.get("app").map(String::as_str))
        .collect::<Vec<_>>();
    apps.sort_unstable();
    check!(
        apps == vec!["inside", "on_the_end", "on_the_start"],
        "got {apps:?}"
    );
}

/// `format_logql_query` returns the canonical spelling of a query, and
/// what "canonical" means differs by the kind of query it is: a stream
/// selector round-trips, a scalar expression is folded to its value, and a
/// vector literal gains an explicit float. Nothing tested any of it --
/// returning an empty string passed the whole suite.
#[test]
pub(crate) fn formatting_a_logql_query_canonicalises_by_kind() {
    let format =
        |query: &str| super::prelude::format_logql_query(query).map_err(|error| error.to_string());

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

/// Nested metric functions are formatted from `krabka-logql`'s recursive
/// AST when the older HTTP-layer shape-specific formatters cannot represent
/// the inner expression.
#[test]
pub(crate) fn formatting_uses_the_recursive_logql_ast_for_nested_expressions() {
    let query = concat!(
        r#"label_replace(label_replace(rate({app="web"}[5m]),"inner","$1","app","(.*)"),"#,
        r#""outer","$1","inner","(.*)")"#,
    );

    check!(
        super::prelude::format_logql_query(query).expect("the nested expression formats")
            == concat!(
                r#"label_replace(label_replace(rate({app="web"}[5m]), "inner", "$1", "app", "(.*)"), "#,
                r#""outer", "$1", "inner", "(.*)")"#,
            )
    );
}

#[test]
pub(crate) fn recursive_formatting_preserves_loki_specific_rejections() {
    let queries = [
        r#"sort(label_join(vector(1),"joined","/","app"))"#,
        r#"(label_join(vector(1),"joined","/","app"))"#,
        "sort(vector(-1))",
    ];

    for query in queries {
        check!(
            super::prelude::format_logql_query(query).is_err(),
            "query: {query}"
        );
    }
}
