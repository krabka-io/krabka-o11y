//! Regex evaluation preserves selector and line-filter semantics across clones.

use assert2::assert;
use krabka_logql::{LabelMatcher, Labels, LineFilter, LineFilterOp, MatchOp};

struct SelectorRegexCase {
    pattern: &'static str,
    candidate: Option<&'static str>,
    positive: bool,
    negative: bool,
    empty: bool,
}

struct LineRegexCase {
    pattern: &'static str,
    line: &'static str,
    matches: bool,
}

#[test]
fn selector_regexes_keep_anchoring_empty_and_missing_label_behavior() {
    for case in [
        SelectorRegexCase {
            pattern: "api|worker",
            candidate: Some("api"),
            positive: true,
            negative: false,
            empty: false,
        },
        SelectorRegexCase {
            pattern: "api|worker",
            candidate: Some("xapi"),
            positive: false,
            negative: true,
            empty: false,
        },
        SelectorRegexCase {
            pattern: "api|worker",
            candidate: Some("worker\n"),
            positive: false,
            negative: true,
            empty: false,
        },
        SelectorRegexCase {
            pattern: ".+",
            candidate: Some("π"),
            positive: true,
            negative: false,
            empty: false,
        },
        SelectorRegexCase {
            pattern: ".*",
            candidate: Some(""),
            positive: true,
            negative: false,
            empty: true,
        },
        SelectorRegexCase {
            pattern: ".*",
            candidate: None,
            positive: true,
            negative: true,
            empty: true,
        },
        SelectorRegexCase {
            pattern: ".+",
            candidate: None,
            positive: false,
            negative: true,
            empty: false,
        },
        SelectorRegexCase {
            pattern: "(?s).+",
            candidate: Some("api\nworker"),
            positive: true,
            negative: false,
            empty: false,
        },
    ] {
        let labels = case.candidate.map_or_else(Labels::new, |value| {
            let mut labels = Labels::new();
            labels.insert("app".to_string(), value.to_string());
            labels
        });
        let matcher = LabelMatcher::new("app", MatchOp::RegexEqual, case.pattern).unwrap();
        let negated = LabelMatcher::new("app", MatchOp::RegexNotEqual, case.pattern).unwrap();
        for _ in 0..3 {
            assert!(matcher.clone().matches(&labels) == case.positive);
            assert!(negated.clone().matches(&labels) == case.negative);
            assert!(matcher.matches_empty_value() == case.empty);
            assert!(negated.matches_empty_value() == !case.empty);
        }
    }
    assert!(LabelMatcher::new("app", MatchOp::RegexEqual, "[").is_err());
}

#[test]
fn public_selector_mutation_uses_the_current_operation_and_value() {
    let original = LabelMatcher::new("app", MatchOp::RegexEqual, "api").unwrap();
    let mut changed = original.clone();
    changed.value = "worker".to_string();
    changed.op = MatchOp::RegexNotEqual;
    assert!(changed == LabelMatcher::new("app", MatchOp::RegexNotEqual, "worker").unwrap());
    let mut api = Labels::new();
    api.insert("app".to_string(), "api".to_string());
    let mut worker = Labels::new();
    worker.insert("app".to_string(), "worker".to_string());
    assert!(original.matches(&api));
    assert!(!original.matches(&worker));
    assert!(changed.matches(&api));
    assert!(!changed.matches(&worker));
    assert!(changed.matches(&Labels::new()));

    let mut equality = LabelMatcher::new("app", MatchOp::Equal, "api|worker").unwrap();
    equality.op = MatchOp::RegexEqual;
    assert!(equality.matches(&api));
    assert!(equality.matches(&worker));
}

#[test]
fn line_regexes_keep_unanchored_matching_negation_and_newline_behavior() {
    for case in [
        LineRegexCase {
            pattern: "accepted",
            line: "prefix accepted suffix",
            matches: true,
        },
        LineRegexCase {
            pattern: "accepted$",
            line: "prefix accepted suffix",
            matches: false,
        },
        LineRegexCase {
            pattern: "accepted$",
            line: "π accepted",
            matches: true,
        },
        LineRegexCase {
            pattern: "^accepted$",
            line: "accepted\n",
            matches: false,
        },
        LineRegexCase {
            pattern: ".+",
            line: "\n",
            matches: false,
        },
        LineRegexCase {
            pattern: "(?s).+",
            line: "\n",
            matches: true,
        },
        LineRegexCase {
            pattern: "",
            line: "",
            matches: true,
        },
    ] {
        let filter = LineFilter::new(LineFilterOp::Regex, case.pattern).unwrap();
        let negated = LineFilter::new(LineFilterOp::NotRegex, case.pattern).unwrap();
        for _ in 0..3 {
            assert!(filter.clone().matches(case.line) == case.matches);
            assert!(negated.clone().matches(case.line) == !case.matches);
        }
    }
    assert!(LineFilter::new(LineFilterOp::Regex, "[").is_err());
}

#[test]
fn public_line_filter_mutation_uses_the_current_operation_and_pattern() {
    let original = LineFilter::new(LineFilterOp::Regex, "accepted$").unwrap();
    let mut changed = original.clone();
    changed.pattern = "ignored$".to_string();
    changed.op = LineFilterOp::NotRegex;
    assert!(changed == LineFilter::new(LineFilterOp::NotRegex, "ignored$").unwrap());
    assert!(original.matches("line accepted"));
    assert!(!original.matches("line ignored"));
    assert!(changed.matches("line accepted"));
    assert!(!changed.matches("line ignored"));

    let mut contains = LineFilter::new(LineFilterOp::Contains, "accepted$").unwrap();
    contains.op = LineFilterOp::Regex;
    assert!(contains.matches("line accepted"));
    assert!(!contains.matches("line accepted suffix"));
}
