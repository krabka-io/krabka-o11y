//! Regex evaluation preserves selector and line-filter semantics across clones.

use assert2::assert;
use krabka_logql::{LabelMatcher, Labels, LineFilter, LineFilterOp, MatchOp};

#[test]
fn selector_regexes_keep_anchoring_empty_and_missing_label_behavior() {
    for (pattern, candidate, positive, negative, empty) in [
        ("api|worker", Some("api"), true, false, false),
        ("api|worker", Some("xapi"), false, true, false),
        ("api|worker", Some("worker\n"), false, true, false),
        (".+", Some("π"), true, false, false),
        (".*", Some(""), true, false, true),
        (".*", None, true, true, true),
        (".+", None, false, true, false),
        ("(?s).+", Some("api\nworker"), true, false, false),
    ] {
        let labels = candidate.map_or_else(Labels::new, |value| {
            Labels::from([("app".to_string(), value.to_string())])
        });
        let matcher = LabelMatcher::new("app", MatchOp::RegexEqual, pattern).unwrap();
        let negated = LabelMatcher::new("app", MatchOp::RegexNotEqual, pattern).unwrap();
        for _ in 0..3 {
            assert!(
                matcher.clone().matches(&labels) == positive,
                "{pattern:?} {candidate:?}"
            );
            assert!(
                negated.clone().matches(&labels) == negative,
                "{pattern:?} {candidate:?}"
            );
            assert!(matcher.matches_empty_value() == empty);
            assert!(negated.matches_empty_value() == !empty);
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
    let api = Labels::from([("app".to_string(), "api".to_string())]);
    let worker = Labels::from([("app".to_string(), "worker".to_string())]);
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
    for (pattern, line, matches) in [
        ("accepted", "prefix accepted suffix", true),
        ("accepted$", "prefix accepted suffix", false),
        ("accepted$", "π accepted", true),
        ("^accepted$", "accepted\n", false),
        (".+", "\n", false),
        ("(?s).+", "\n", true),
        ("", "", true),
    ] {
        let filter = LineFilter::new(LineFilterOp::Regex, pattern).unwrap();
        let negated = LineFilter::new(LineFilterOp::NotRegex, pattern).unwrap();
        for _ in 0..3 {
            assert!(
                filter.clone().matches(line) == matches,
                "{pattern:?} {line:?}"
            );
            assert!(
                negated.clone().matches(line) == !matches,
                "{pattern:?} {line:?}"
            );
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
