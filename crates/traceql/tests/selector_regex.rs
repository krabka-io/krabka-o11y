use assert2::assert;
use krabka_traceql::{
    AttrValue, Attribute, EventRef, LinkRef, MatchCmp, MatchScope, MatchValue, SpanMatcher,
    attr_matches, attr_values_match, event_matcher_matches_absence, event_matcher_matches_event,
    link_matcher_matches_absence, link_matcher_matches_link, string_matches,
};

fn matcher(pattern: &str, op: MatchCmp) -> SpanMatcher {
    SpanMatcher {
        scope: MatchScope::Span,
        key: "method".into(),
        op,
        value: MatchValue::Str(pattern.into()),
        negated: false,
    }
}

#[test]
fn prepared_strings_preserve_regex_semantics() {
    for (pattern, value, re, nre) in [
        ("GET.*", "GET /", true, false),
        ("GET", "GET /", false, true),
        ("GET|POST", "xGET", false, true),
        ("GET|POST", "POST", true, false),
        (r"\p{L}+", "日本語é", true, false),
        (r"\p{L}+", "123", false, true),
        ("", "", true, false),
        ("", "x", false, true),
        (".*", "", true, false),
        ("[", "", false, false),
        ("[", "anything", false, false),
        ("(?=x)", "x", false, false),
        ("x", "x\n", false, true),
    ] {
        for (op, expected) in [(MatchCmp::Re, re), (MatchCmp::Nre, nre)] {
            let original = [matcher(pattern, op)];
            let prepared = SpanMatcher::prepare_regexes(&original);
            assert!(string_matches(value, op, &prepared[0].value) == expected);
            assert!(string_matches(value, op, &original[0].value) == expected);
        }
    }
}

#[test]
fn prepared_arrays_preserve_quantifiers_and_absence() {
    let string = |value: &str| AttrValue::Str(value.into());
    for (value, re, nre) in [
        (AttrValue::Array(vec![]), false, false),
        (AttrValue::Array(vec![string("GET")]), true, false),
        (AttrValue::Array(vec![string("POST")]), false, true),
        (
            AttrValue::Array(vec![string("GET"), string("POST")]),
            true,
            false,
        ),
        (
            AttrValue::Array(vec![string("POST"), string("PUT")]),
            false,
            true,
        ),
        (
            AttrValue::Array(vec![string("POST"), AttrValue::Int(1)]),
            false,
            false,
        ),
        (
            AttrValue::Array(vec![AttrValue::Array(vec![string("GET")])]),
            true,
            false,
        ),
    ] {
        for (op, expected) in [(MatchCmp::Re, re), (MatchCmp::Nre, nre)] {
            let original = [matcher("GET", op)];
            let prepared = SpanMatcher::prepare_regexes(&original);
            assert!(attr_matches(&value, op, &prepared[0].value) == expected);
            assert!(!attr_values_match(&[], op, &prepared[0].value));
            let invalid = [matcher("[", op)];
            let invalid = SpanMatcher::prepare_regexes(&invalid);
            assert!(!attr_matches(&value, op, &invalid[0].value));
        }
    }
    for (op, expected) in [(MatchCmp::Eq, true), (MatchCmp::Neq, false)] {
        assert!(attr_matches(&AttrValue::Array(vec![]), op, &MatchValue::Nil) == expected);
        assert!(attr_values_match(&[], op, &MatchValue::Nil) == expected);
    }
}

#[test]
fn prepared_nested_matchers_preserve_negation() {
    let attributes = vec![Attribute::new(
        "method",
        AttrValue::Array(vec![
            AttrValue::Str("GET".into()),
            AttrValue::Str("POST".into()),
        ]),
    )];
    let event = EventRef {
        time_since_start: krabka_units::Time::default(),
        name: "GET".into(),
        attributes: attributes.clone(),
    };
    let link = LinkRef {
        trace_id: [0xab; 16],
        span_id: [0xcd; 8],
        attributes,
    };
    for (scope, key, pattern) in [
        (MatchScope::Event, "method", "GET"),
        (MatchScope::Link, "method", "GET"),
        (MatchScope::Intrinsic, "event:name", "GET"),
        (MatchScope::Intrinsic, "link:traceID", "ab.*"),
        (MatchScope::Intrinsic, "link:spanID", "cd.*"),
    ] {
        for (pattern, valid) in [(pattern, true), ("[", false)] {
            for op in [MatchCmp::Re, MatchCmp::Nre] {
                for negated in [false, true] {
                    let original = [SpanMatcher {
                        scope,
                        key: key.into(),
                        op,
                        value: MatchValue::Str(pattern.into()),
                        negated,
                    }];
                    let prepared = SpanMatcher::prepare_regexes(&original);
                    let matcher = &prepared[0];
                    let expected = (valid && op == MatchCmp::Re) != negated;
                    if scope == MatchScope::Event || key.starts_with("event:") {
                        assert!(event_matcher_matches_event(&event, matcher) == expected);
                        assert!(event_matcher_matches_absence(matcher) == negated);
                    } else {
                        assert!(link_matcher_matches_link(&link, matcher) == expected);
                        assert!(link_matcher_matches_absence(matcher) == negated);
                    }
                }
            }
        }
    }
}
