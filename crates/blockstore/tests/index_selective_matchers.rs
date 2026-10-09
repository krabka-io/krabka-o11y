use std::collections::BTreeSet;

use assert2::assert;
use krabka_blockstore::{Index, LabelMatcher, Labels, MatchOp};

fn fixture() -> Index {
    let mut index = Index::new();
    for (fingerprint, pairs) in [
        (11, vec![("app", "api"), ("region", "west"), ("zone", "")]),
        (12, vec![("app", "api"), ("region", "east")]),
        (13, vec![("app", "api"), ("region", "東京")]),
        (14, vec![("app", "worker")]),
        (15, vec![("app", "worker"), ("zone", "west")]),
        (16, vec![("app", "api"), ("region", "west\nzone\0")]),
        (17, vec![("region", "west")]),
        (18, vec![("app", "")]),
        (19, vec![("app", "api"), ("zone", "非空")]),
    ] {
        let mut labels = Labels::from_pairs(pairs);
        labels.insert("all", "series");
        index.add_series("tenant", fingerprint, &labels);
    }
    index.add_series("other", 10, &Labels::from_pairs([("app", "api")]));
    index
}

#[test]
fn regex_posting_unions_preserve_missing_empty_and_multiline_labels() {
    let index = fixture();
    for (name, op, pattern, expected) in [
        ("region", MatchOp::Re, "west|east", vec![11, 12, 17]),
        ("region", MatchOp::Re, ".+", vec![11, 12, 13, 17]),
        ("region", MatchOp::Re, "(?s:.+)", vec![11, 12, 13, 16, 17]),
        ("region", MatchOp::Nre, "", vec![11, 12, 13, 16, 17]),
        ("zone", MatchOp::Nre, "", vec![15, 19]),
        ("zone", MatchOp::Re, "west|非空", vec![15, 19]),
        ("region", MatchOp::Re, "東京|west\\nzone\\x00", vec![13, 16]),
        (
            "app",
            MatchOp::Re,
            "api|worker",
            vec![11, 12, 13, 14, 15, 16, 19],
        ),
    ] {
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        let matcher = LabelMatcher::new(name, op, pattern);
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
    for (matcher, expected) in [
        (
            LabelMatcher::new("zone", MatchOp::Re, ".*"),
            vec![11, 12, 13, 14, 15, 16, 19],
        ),
        (LabelMatcher::new("region", MatchOp::Nre, ".*"), vec![16]),
        (
            LabelMatcher::new("missing", MatchOp::Re, ".*"),
            vec![11, 12, 13, 14, 15, 16, 19],
        ),
    ] {
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        for matchers in [
            vec![anchor.clone(), matcher.clone()],
            vec![matcher, anchor.clone()],
        ] {
            assert!(index.resolve("tenant", &matchers).unwrap() == expected);
        }
    }
    let all = LabelMatcher::new("all", MatchOp::Eq, "series");
    for (matcher, expected) in [
        (
            LabelMatcher::new("zone", MatchOp::Re, ".*"),
            vec![11, 12, 13, 14, 15, 16, 17, 18, 19],
        ),
        (
            LabelMatcher::new("zone", MatchOp::Re, ""),
            vec![11, 12, 13, 14, 16, 17, 18],
        ),
        (LabelMatcher::new("region", MatchOp::Nre, ".*"), vec![16]),
    ] {
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        for matchers in [
            vec![all.clone(), matcher.clone()],
            vec![matcher, all.clone()],
        ] {
            assert!(index.resolve("tenant", &matchers).unwrap() == expected);
        }
    }
}

#[test]
fn selective_resolution_preserves_full_matcher_ledger_in_each_order() {
    let index = fixture();
    let anchor = LabelMatcher::new("app", MatchOp::Eq, "api");
    for (matcher, expected) in [
        (
            LabelMatcher::new("region", MatchOp::Neq, "west"),
            vec![12, 13, 16, 19],
        ),
        (LabelMatcher::new("region", MatchOp::Eq, ""), vec![19]),
        (
            LabelMatcher::new("zone", MatchOp::Eq, ""),
            vec![11, 12, 13, 16],
        ),
        (LabelMatcher::new("zone", MatchOp::Neq, ""), vec![19]),
        (
            LabelMatcher::new("zone", MatchOp::Re, ""),
            vec![11, 12, 13, 16],
        ),
        (LabelMatcher::new("zone", MatchOp::Nre, ""), vec![19]),
        (
            LabelMatcher::new("region", MatchOp::Re, ".+"),
            vec![11, 12, 13],
        ),
        (
            LabelMatcher::new("region", MatchOp::Nre, "west|east"),
            vec![13, 16, 19],
        ),
        (LabelMatcher::new("region", MatchOp::Re, "east"), vec![12]),
        (
            LabelMatcher::new("region", MatchOp::Re, "東京|west\nzone\x00"),
            vec![13, 16],
        ),
        (LabelMatcher::new("region", MatchOp::Nre, ".*"), vec![16]),
        (
            LabelMatcher::new("region", MatchOp::Re, ".*"),
            vec![11, 12, 13, 19],
        ),
        (LabelMatcher::new("app", MatchOp::Eq, "worker"), vec![]),
        (LabelMatcher::new("missing", MatchOp::Eq, "value"), vec![]),
        (
            LabelMatcher::new("missing", MatchOp::Re, ".*"),
            vec![11, 12, 13, 16, 19],
        ),
        (
            LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
            vec![12, 16],
        ),
        (
            LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
            vec![11, 13, 19],
        ),
    ] {
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        for matchers in [
            vec![anchor.clone(), matcher.clone()],
            vec![matcher.clone(), anchor.clone()],
        ] {
            assert!(index.resolve("tenant", &matchers).unwrap() == expected);
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
    index.add_series("tenant", 11, &Labels::from_pairs([("app", "replacement")]));
    index.add_series("tenant", 20, &Labels::from_pairs([("app", "api")]));
    let matchers = [
        LabelMatcher::new("region", MatchOp::Neq, "east"),
        LabelMatcher::new("app", MatchOp::Eq, "api"),
    ];
    assert!(index.resolve("tenant", &matchers).unwrap() == BTreeSet::from([11, 13, 16, 19, 20]));
    assert!(snapshot.resolve("tenant", &matchers).unwrap() == BTreeSet::from([11, 13, 16, 19]));
    assert!(index.resolve("other", &matchers).unwrap() == BTreeSet::from([10]));
}
