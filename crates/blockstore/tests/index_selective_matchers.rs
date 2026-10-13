use std::collections::BTreeSet;

use assert2::assert;
use krabka_blockstore::{Index, LabelMatcher, LabelPair, Labels, MatchOp};

struct SeriesFixture {
    fingerprint: u64,
    labels: Vec<LabelPair>,
}

struct MatcherExpectedCase {
    matcher: LabelMatcher,
    expected: Vec<u64>,
}

struct RegexCase {
    name: &'static str,
    op: MatchOp,
    pattern: &'static str,
    expected: Vec<u64>,
}

fn matcher_expected(matcher: LabelMatcher, expected: &[u64]) -> MatcherExpectedCase {
    MatcherExpectedCase {
        matcher,
        expected: expected.to_vec(),
    }
}

fn assert_resolves_in_each_order(
    index: &Index,
    tenant: &str,
    expected: &BTreeSet<u64>,
    orders: [Vec<LabelMatcher>; 2],
) {
    for matchers in orders {
        assert!(&index.resolve(tenant, &matchers).unwrap() == expected);
    }
}

fn fixture() -> Index {
    let mut index = Index::new();
    for fixture in [
        SeriesFixture {
            fingerprint: 11,
            labels: vec![
                LabelPair::new("app", "api"),
                LabelPair::new("region", "west"),
                LabelPair::new("zone", ""),
            ],
        },
        SeriesFixture {
            fingerprint: 12,
            labels: vec![
                LabelPair::new("app", "api"),
                LabelPair::new("region", "east"),
            ],
        },
        SeriesFixture {
            fingerprint: 13,
            labels: vec![
                LabelPair::new("app", "api"),
                LabelPair::new("region", "東京"),
            ],
        },
        SeriesFixture {
            fingerprint: 14,
            labels: vec![LabelPair::new("app", "worker")],
        },
        SeriesFixture {
            fingerprint: 15,
            labels: vec![
                LabelPair::new("app", "worker"),
                LabelPair::new("zone", "west"),
            ],
        },
        SeriesFixture {
            fingerprint: 16,
            labels: vec![
                LabelPair::new("app", "api"),
                LabelPair::new("region", "west\nzone\0"),
            ],
        },
        SeriesFixture {
            fingerprint: 17,
            labels: vec![LabelPair::new("region", "west")],
        },
        SeriesFixture {
            fingerprint: 18,
            labels: vec![LabelPair::new("app", "")],
        },
        SeriesFixture {
            fingerprint: 19,
            labels: vec![LabelPair::new("app", "api"), LabelPair::new("zone", "非空")],
        },
    ] {
        let mut labels = Labels::from_pairs(fixture.labels);
        labels.insert("all", "series");
        index.add_series("tenant", fixture.fingerprint, &labels);
    }
    index.add_series(
        "other",
        10,
        &Labels::from_pairs([LabelPair::new("app", "api")]),
    );
    index
}

#[test]
fn regex_posting_unions_preserve_missing_empty_and_multiline_labels() {
    let index = fixture();
    for case in [
        RegexCase {
            name: "region",
            op: MatchOp::Re,
            pattern: "west|east",
            expected: vec![11, 12, 17],
        },
        RegexCase {
            name: "region",
            op: MatchOp::Re,
            pattern: ".+",
            expected: vec![11, 12, 13, 17],
        },
        RegexCase {
            name: "region",
            op: MatchOp::Re,
            pattern: "(?s:.+)",
            expected: vec![11, 12, 13, 16, 17],
        },
        RegexCase {
            name: "region",
            op: MatchOp::Nre,
            pattern: "",
            expected: vec![11, 12, 13, 16, 17],
        },
        RegexCase {
            name: "zone",
            op: MatchOp::Nre,
            pattern: "",
            expected: vec![15, 19],
        },
        RegexCase {
            name: "zone",
            op: MatchOp::Re,
            pattern: "west|非空",
            expected: vec![15, 19],
        },
        RegexCase {
            name: "region",
            op: MatchOp::Re,
            pattern: "東京|west\\nzone\\x00",
            expected: vec![13, 16],
        },
        RegexCase {
            name: "app",
            op: MatchOp::Re,
            pattern: "api|worker",
            expected: vec![11, 12, 13, 14, 15, 16, 19],
        },
    ] {
        let expected = case.expected.into_iter().collect::<BTreeSet<_>>();
        let matcher = LabelMatcher::new(case.name, case.op, case.pattern);
        let all = LabelMatcher::new("all", MatchOp::Eq, "series");
        for matchers in [
            vec![matcher.clone()],
            vec![all.clone(), matcher.clone()],
            vec![matcher, all],
        ] {
            assert!(index.resolve("tenant", &matchers).unwrap() == expected);
        }
    }
    let anchor = LabelMatcher::new("app", MatchOp::Re, "api|worker");
    for case in [
        matcher_expected(
            LabelMatcher::new("zone", MatchOp::Re, ".*"),
            &[11, 12, 13, 14, 15, 16, 19],
        ),
        matcher_expected(LabelMatcher::new("region", MatchOp::Nre, ".*"), &[16]),
        matcher_expected(
            LabelMatcher::new("missing", MatchOp::Re, ".*"),
            &[11, 12, 13, 14, 15, 16, 19],
        ),
    ] {
        let expected = case.expected.into_iter().collect::<BTreeSet<_>>();
        let matcher = case.matcher;
        assert_resolves_in_each_order(
            &index,
            "tenant",
            &expected,
            [
                vec![anchor.clone(), matcher.clone()],
                vec![matcher, anchor.clone()],
            ],
        );
    }
    let all = LabelMatcher::new("all", MatchOp::Eq, "series");
    for case in [
        matcher_expected(
            LabelMatcher::new("zone", MatchOp::Re, ".*"),
            &[11, 12, 13, 14, 15, 16, 17, 18, 19],
        ),
        matcher_expected(
            LabelMatcher::new("zone", MatchOp::Re, ""),
            &[11, 12, 13, 14, 16, 17, 18],
        ),
        matcher_expected(LabelMatcher::new("region", MatchOp::Nre, ".*"), &[16]),
    ] {
        let expected = case.expected.into_iter().collect::<BTreeSet<_>>();
        let matcher = case.matcher;
        assert_resolves_in_each_order(
            &index,
            "tenant",
            &expected,
            [
                vec![all.clone(), matcher.clone()],
                vec![matcher, all.clone()],
            ],
        );
    }
}

#[test]
fn selective_resolution_preserves_full_matcher_ledger_in_each_order() {
    let index = fixture();
    let anchor = LabelMatcher::new("app", MatchOp::Eq, "api");
    for case in [
        matcher_expected(
            LabelMatcher::new("region", MatchOp::Neq, "west"),
            &[12, 13, 16, 19],
        ),
        matcher_expected(LabelMatcher::new("region", MatchOp::Eq, ""), &[19]),
        matcher_expected(
            LabelMatcher::new("zone", MatchOp::Eq, ""),
            &[11, 12, 13, 16],
        ),
        matcher_expected(LabelMatcher::new("zone", MatchOp::Neq, ""), &[19]),
        matcher_expected(
            LabelMatcher::new("zone", MatchOp::Re, ""),
            &[11, 12, 13, 16],
        ),
        matcher_expected(LabelMatcher::new("zone", MatchOp::Nre, ""), &[19]),
        matcher_expected(
            LabelMatcher::new("region", MatchOp::Re, ".+"),
            &[11, 12, 13],
        ),
        matcher_expected(
            LabelMatcher::new("region", MatchOp::Nre, "west|east"),
            &[13, 16, 19],
        ),
        matcher_expected(LabelMatcher::new("region", MatchOp::Re, "east"), &[12]),
        matcher_expected(
            LabelMatcher::new("region", MatchOp::Re, "東京|west\nzone\x00"),
            &[13, 16],
        ),
        matcher_expected(LabelMatcher::new("region", MatchOp::Nre, ".*"), &[16]),
        matcher_expected(
            LabelMatcher::new("region", MatchOp::Re, ".*"),
            &[11, 12, 13, 19],
        ),
        matcher_expected(LabelMatcher::new("app", MatchOp::Eq, "worker"), &[]),
        matcher_expected(LabelMatcher::new("missing", MatchOp::Eq, "value"), &[]),
        matcher_expected(
            LabelMatcher::new("missing", MatchOp::Re, ".*"),
            &[11, 12, 13, 16, 19],
        ),
        matcher_expected(
            LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
            &[12, 16],
        ),
        matcher_expected(
            LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
            &[11, 13, 19],
        ),
    ] {
        let expected = case.expected.into_iter().collect::<BTreeSet<_>>();
        let matcher = case.matcher;
        let orders = [
            vec![anchor.clone(), matcher.clone()],
            vec![matcher.clone(), anchor.clone()],
        ];
        assert_resolves_in_each_order(&index, "tenant", &expected, orders.clone());
        for matchers in orders {
            assert!(index.resolve("missing", &matchers).unwrap().is_empty());
        }
        // A one-series posting also exercises candidate label evaluation for
        // regexes whose distinct-value posting scan wins on the broader set.
        let narrow = LabelMatcher::new("region", MatchOp::Eq, "west\nzone\0");
        let narrow_expected = expected
            .intersection(&BTreeSet::from([16]))
            .copied()
            .collect();
        for matchers in [
            vec![anchor.clone(), matcher.clone(), narrow.clone()],
            vec![matcher.clone(), narrow.clone(), anchor.clone()],
            vec![narrow, anchor.clone(), matcher],
        ] {
            assert!(index.resolve("tenant", &matchers).unwrap() == narrow_expected);
        }
    }
}

#[test]
fn selective_resolution_preserves_validation_and_short_circuit_errors() {
    let index = fixture();
    let anchor = LabelMatcher::new("app", MatchOp::Eq, "api");
    let empty = LabelMatcher::new("missing", MatchOp::Eq, "value");
    for invalid in [
        LabelMatcher::new("region", MatchOp::Re, "["),
        LabelMatcher::new("region", MatchOp::Nre, "["),
        LabelMatcher::new("__query_shard__", MatchOp::Eq, "0_of_2"),
        LabelMatcher::new("__query_shard__", MatchOp::Re, "1_of_2"),
    ] {
        // The first two stages run even when the first posting is empty.
        assert!(
            index
                .resolve("tenant", &[empty.clone(), invalid.clone()])
                .is_err()
        );
        assert!(
            index
                .resolve("tenant", &[anchor.clone(), invalid.clone(), empty.clone()])
                .is_err()
        );
        // A later error is not reached after the second stage empties the set.
        assert!(
            index
                .resolve("tenant", &[anchor.clone(), empty.clone(), invalid])
                .unwrap()
                .is_empty()
        );
    }
    for matchers in [
        vec![],
        vec![LabelMatcher::new("app", MatchOp::Eq, "")],
        vec![LabelMatcher::new("zone", MatchOp::Neq, "west")],
        vec![LabelMatcher::new("zone", MatchOp::Re, ".*")],
        vec![LabelMatcher::new("zone", MatchOp::Nre, "west")],
        vec![LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2")],
    ] {
        assert!(index.resolve("tenant", &matchers).is_err());
    }
}

#[test]
fn selective_resolution_uses_tenant_labels_and_keeps_snapshots_independent() {
    let mut index = fixture();
    let snapshot = index.clone();
    index.add_series(
        "tenant",
        11,
        &Labels::from_pairs([LabelPair::new("app", "replacement")]),
    );
    index.add_series(
        "tenant",
        20,
        &Labels::from_pairs([LabelPair::new("app", "api")]),
    );
    let matchers = [
        LabelMatcher::new("region", MatchOp::Neq, "east"),
        LabelMatcher::new("app", MatchOp::Eq, "api"),
    ];
    assert!(index.resolve("tenant", &matchers).unwrap() == BTreeSet::from([11, 13, 16, 19, 20]));
    assert!(snapshot.resolve("tenant", &matchers).unwrap() == BTreeSet::from([11, 13, 16, 19]));
    assert!(index.resolve("other", &matchers).unwrap() == BTreeSet::from([10]));
}
