use std::collections::BTreeSet;

use assert2::assert;
use krabka_blockstore::{LabelIndex, LabelPredicate, LogMatchOp, labels};

struct MatchCase {
    name: &'static str,
    op: LogMatchOp,
    value: &'static str,
    expected: Vec<u64>,
}

#[test]
fn exact_posting_order_preserves_log_matcher_ledger() {
    let mut index = LabelIndex::default();
    let selected = index.insert_series(
        "tenant",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("pod", "chosen"),
            krabka_blockstore::LabelPair::new("zone", ""),
        ]),
    );
    let other = index.insert_series(
        "tenant",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("pod", "other"),
        ]),
    );
    let worker = index.insert_series(
        "tenant",
        labels([
            krabka_blockstore::LabelPair::new("app", "worker"),
            krabka_blockstore::LabelPair::new("zone", "東京"),
        ]),
    );
    index.insert_series(
        "other",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("pod", "chosen"),
        ]),
    );
    let api = LabelPredicate::new("app", LogMatchOp::Equal, "api").unwrap();
    let pod = LabelPredicate::new("pod", LogMatchOp::Equal, "chosen").unwrap();
    for case in [
        MatchCase {
            name: "pod",
            op: LogMatchOp::Equal,
            value: "chosen",
            expected: vec![selected],
        },
        MatchCase {
            name: "pod",
            op: LogMatchOp::Equal,
            value: "missing",
            expected: vec![],
        },
        MatchCase {
            name: "app",
            op: LogMatchOp::Equal,
            value: "worker",
            expected: vec![],
        },
        MatchCase {
            name: "zone",
            op: LogMatchOp::Equal,
            value: "",
            expected: vec![selected],
        },
        MatchCase {
            name: "zone",
            op: LogMatchOp::NotEqual,
            value: "",
            expected: vec![other],
        },
        MatchCase {
            name: "pod",
            op: LogMatchOp::NotEqual,
            value: "other",
            expected: vec![selected],
        },
        MatchCase {
            name: "missing",
            op: LogMatchOp::NotEqual,
            value: "",
            expected: vec![selected, other],
        },
        MatchCase {
            name: "missing",
            op: LogMatchOp::NotEqual,
            value: "value",
            expected: vec![selected, other],
        },
        MatchCase {
            name: "pod",
            op: LogMatchOp::RegexEqual,
            value: "chosen|other",
            expected: vec![selected, other],
        },
        MatchCase {
            name: "pod",
            op: LogMatchOp::RegexNotEqual,
            value: "other",
            expected: vec![selected],
        },
        MatchCase {
            name: "pod",
            op: LogMatchOp::RegexEqual,
            value: "chos",
            expected: vec![],
        },
        MatchCase {
            name: "missing",
            op: LogMatchOp::RegexEqual,
            value: ".*",
            expected: vec![selected, other],
        },
    ] {
        let predicate = LabelPredicate::new(case.name, case.op, case.value).unwrap();
        let expected = case.expected.into_iter().collect::<BTreeSet<_>>();
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
    let original = labels([
        krabka_blockstore::LabelPair::new("app", "api"),
        krabka_blockstore::LabelPair::new("x", "a\0b"),
        krabka_blockstore::LabelPair::new("zone", "東京"),
    ]);
    let selected = index.insert_series("tenant", original);
    index.insert_series(
        "tenant",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("x\0a", "b"),
        ]),
    );
    let snapshot = index.clone();
    let added = index.insert_series(
        "tenant",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("x", "a\0b"),
            krabka_blockstore::LabelPair::new("zone", "west"),
        ]),
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
