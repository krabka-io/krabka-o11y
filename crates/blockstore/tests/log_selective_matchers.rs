use std::collections::BTreeSet;

use assert2::assert;
use krabka_blockstore::{LabelIndex, LabelPredicate, LogMatchOp, labels};

#[test]
fn exact_posting_order_preserves_log_matcher_ledger() {
    let mut index = LabelIndex::default();
    let selected = index.insert_series(
        "tenant",
        labels([("app", "api"), ("pod", "chosen"), ("zone", "")]),
    );
    let other = index.insert_series("tenant", labels([("app", "api"), ("pod", "other")]));
    let worker = index.insert_series("tenant", labels([("app", "worker"), ("zone", "東京")]));
    index.insert_series("other", labels([("app", "api"), ("pod", "chosen")]));
    let api = LabelPredicate::new("app", LogMatchOp::Equal, "api").unwrap();
    let pod = LabelPredicate::new("pod", LogMatchOp::Equal, "chosen").unwrap();
    for (name, op, value, expected) in [
        ("pod", LogMatchOp::Equal, "chosen", vec![selected]),
        ("pod", LogMatchOp::Equal, "missing", vec![]),
        ("app", LogMatchOp::Equal, "worker", vec![]),
        ("zone", LogMatchOp::Equal, "", vec![selected]),
        ("zone", LogMatchOp::NotEqual, "", vec![other]),
        ("pod", LogMatchOp::NotEqual, "other", vec![selected]),
        ("missing", LogMatchOp::NotEqual, "", vec![selected, other]),
        (
            "missing",
            LogMatchOp::NotEqual,
            "value",
            vec![selected, other],
        ),
        (
            "pod",
            LogMatchOp::RegexEqual,
            "chosen|other",
            vec![selected, other],
        ),
        ("pod", LogMatchOp::RegexNotEqual, "other", vec![selected]),
        ("pod", LogMatchOp::RegexEqual, "chos", vec![]),
        (
            "missing",
            LogMatchOp::RegexEqual,
            ".*",
            vec![selected, other],
        ),
    ] {
        let predicate = LabelPredicate::new(name, op, value).unwrap();
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        for predicates in [
            vec![api.clone(), predicate.clone()],
            vec![predicate, api.clone()],
        ] {
            assert!(index.match_series("tenant", &predicates) == expected);
            assert!(index.match_series("missing", &predicates).is_empty());
        }
    }
    for predicates in [
        vec![api.clone(), pod.clone(), api.clone()],
        vec![pod.clone(), api.clone(), pod],
    ] {
        assert!(index.match_series("tenant", &predicates) == BTreeSet::from([selected]));
    }
    assert!(index.match_series("tenant", &[]) == BTreeSet::from([selected, other, worker]));
    let only_worker = LabelPredicate::new("app", LogMatchOp::NotEqual, "api").unwrap();
    assert!(index.match_series("tenant", &[only_worker]) == BTreeSet::from([worker]));
    let absent = LabelPredicate::new("missing", LogMatchOp::NotEqual, "").unwrap();
    assert!(index.match_series("tenant", &[absent]) == BTreeSet::from([selected, other, worker]));
    assert!(index.match_series("other", &[api]).len() == 1);
    assert!(
        index.label_names("tenant") == BTreeSet::from(["app".into(), "pod".into(), "zone".into()])
    );
    assert!(index.label_names("other") == BTreeSet::from(["app".into(), "pod".into()]));
    assert!(
        index.label_values("tenant", "pod") == BTreeSet::from(["chosen".into(), "other".into()])
    );
    assert!(index.label_values("tenant", "zone") == BTreeSet::from([String::new(), "東京".into()]));
    assert!(index.label_names("missing").is_empty());
    assert!(index.label_values("missing", "app").is_empty());
    assert!(index.label_values("tenant", "missing").is_empty());
}

#[test]
fn restrictive_log_postings_preserve_unicode_nul_and_snapshots() {
    let mut index = LabelIndex::default();
    let original = labels([("app", "api"), ("x", "a\0b"), ("zone", "東京")]);
    let selected = index.insert_series("tenant", original);
    index.insert_series("tenant", labels([("app", "api"), ("x\0a", "b")]));
    let snapshot = index.clone();
    let added = index.insert_series(
        "tenant",
        labels([("app", "api"), ("x", "a\0b"), ("zone", "west")]),
    );
    let predicates = [
        LabelPredicate::new("app", LogMatchOp::Equal, "api").unwrap(),
        LabelPredicate::new("x", LogMatchOp::Equal, "a\0b").unwrap(),
    ];
    assert!(index.match_series("tenant", &predicates) == BTreeSet::from([selected, added]));
    assert!(snapshot.match_series("tenant", &predicates) == BTreeSet::from([selected]));
    assert!(index.label_values("tenant", "x") == BTreeSet::from(["a\0b".into()]));
    assert!(index.label_values("tenant", "x\0a") == BTreeSet::from(["b".into()]));
}
