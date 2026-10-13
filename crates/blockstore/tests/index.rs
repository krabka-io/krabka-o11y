use std::collections::BTreeSet;

use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LabelPredicate, LogBlockIndex as BlockIndex,
    LogMatchOp as MatchOp, TimeRange, labels, series_fingerprint,
};

struct PredicateCase {
    candidate: krabka_blockstore::LogLabels,
    want: bool,
}

struct SeriesLabelsCase<'a> {
    tenant: &'a str,
    series_id: u64,
    want: Option<&'a krabka_blockstore::LogLabels>,
}

#[test]
fn series_fingerprint_is_stable_across_label_ordering() {
    let left = labels([
        krabka_blockstore::LabelPair::new("env", "prod"),
        krabka_blockstore::LabelPair::new("app", "api"),
    ]);
    let right = labels([
        krabka_blockstore::LabelPair::new("app", "api"),
        krabka_blockstore::LabelPair::new("env", "prod"),
    ]);

    assert2::assert!(series_fingerprint(&left) == series_fingerprint(&right));
}

#[test]
fn label_index_is_tenant_scoped_and_applies_all_matcher_ops() {
    let mut index = LabelIndex::default();
    let api_prod = index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );
    index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("env", "dev"),
        ]),
    );
    let worker_prod = index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "worker"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );
    index.insert_series(
        "tenant-b",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );

    let matched = index.match_series(
        "tenant-a",
        &[
            LabelPredicate::new("app", MatchOp::RegexEqual, "api|worker").unwrap(),
            LabelPredicate::new("env", MatchOp::NotEqual, "dev").unwrap(),
            LabelPredicate::new("app", MatchOp::RegexNotEqual, "admin").unwrap(),
        ],
    );

    assert2::assert!(matched == BTreeSet::from([api_prod, worker_prod]));
}

#[test]
fn label_predicate_equal_matches_only_identical_label_value() {
    let predicate = LabelPredicate::new("app", MatchOp::Equal, "api").unwrap();
    let matching = labels([
        krabka_blockstore::LabelPair::new("app", "api"),
        krabka_blockstore::LabelPair::new("env", "prod"),
    ]);
    let different = labels([
        krabka_blockstore::LabelPair::new("app", "worker"),
        krabka_blockstore::LabelPair::new("env", "prod"),
    ]);

    for case in [
        PredicateCase {
            candidate: matching,
            want: true,
        },
        PredicateCase {
            candidate: different,
            want: false,
        },
        PredicateCase {
            candidate: labels([krabka_blockstore::LabelPair::new("env", "prod")]),
            want: false,
        },
    ] {
        assert2::assert!(predicate.matches(&case.candidate) == case.want);
    }
}

#[test]
fn label_index_exact_match_requires_posting_candidate_set() {
    let mut index = LabelIndex::default();
    let api = index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );
    index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "worker"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );

    let matched = index.match_series(
        "tenant-a",
        &[LabelPredicate::new("app", MatchOp::Equal, "api").unwrap()],
    );

    assert2::assert!(matched == BTreeSet::from([api]));
}

#[test]
fn label_metadata_is_tenant_scoped() {
    let mut index = LabelIndex::default();
    index.insert_series(
        "tenant-a",
        labels([
            krabka_blockstore::LabelPair::new("app", "api"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );
    index.insert_series(
        "tenant-b",
        labels([
            krabka_blockstore::LabelPair::new("service", "billing"),
            krabka_blockstore::LabelPair::new("env", "prod"),
        ]),
    );

    assert2::assert!(index.label_names("tenant-a") == BTreeSet::from(["app".into(), "env".into()]));
    assert2::assert!(index.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    assert2::assert!(
        index.label_names("tenant-b") == BTreeSet::from(["env".into(), "service".into()])
    );
    assert2::assert!(index.label_values("tenant-b", "app") == BTreeSet::new());
}

#[test]
fn label_index_returns_labels_and_tenant_series() {
    let mut index = LabelIndex::default();
    let api_labels = labels([
        krabka_blockstore::LabelPair::new("app", "api"),
        krabka_blockstore::LabelPair::new("env", "prod"),
    ]);
    let worker_labels = labels([
        krabka_blockstore::LabelPair::new("app", "worker"),
        krabka_blockstore::LabelPair::new("env", "prod"),
    ]);
    let api = index.insert_series("tenant-a", api_labels.clone());
    let worker = index.insert_series("tenant-a", worker_labels.clone());
    index.insert_series(
        "tenant-b",
        labels([krabka_blockstore::LabelPair::new("app", "api")]),
    );

    for case in [
        SeriesLabelsCase {
            tenant: "tenant-a",
            series_id: api,
            want: Some(&api_labels),
        },
        SeriesLabelsCase {
            tenant: "tenant-a",
            series_id: worker,
            want: Some(&worker_labels),
        },
        SeriesLabelsCase {
            tenant: "tenant-a",
            series_id: 0,
            want: None,
        },
        SeriesLabelsCase {
            tenant: "tenant-b",
            series_id: api,
            want: None,
        },
    ] {
        assert2::assert!(index.labels_for(case.tenant, case.series_id) == case.want);
    }

    let mut expected = vec![
        krabka_blockstore::TenantSeries {
            fingerprint: api,
            labels: api_labels,
        },
        krabka_blockstore::TenantSeries {
            fingerprint: worker,
            labels: worker_labels,
        },
    ];
    expected.sort_by_key(|series| series.fingerprint);
    assert2::assert!(index.tenant_series("tenant-a") == expected);
    assert2::assert!(index.tenant_series("missing") == Vec::new());
}

#[test]
fn block_index_prunes_by_tenant_time_and_fingerprint() {
    let mut series = LabelIndex::default();
    let api = series.insert_series(
        "tenant-a",
        labels([krabka_blockstore::LabelPair::new("app", "api")]),
    );
    let worker = series.insert_series(
        "tenant-a",
        labels([krabka_blockstore::LabelPair::new("app", "worker")]),
    );
    let other_tenant_api = series.insert_series(
        "tenant-b",
        labels([krabka_blockstore::LabelPair::new("app", "api")]),
    );

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 21, 30, TimeRange::new(300, 399).unwrap()),
        BTreeSet::from([worker]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-b", 1, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([other_tenant_api]),
    ));

    let matched = blocks.match_blocks("tenant-a", TimeRange::new(150, 350).unwrap(), &[api]);

    assert2::assert!(
        matched
            == vec![BlockDescriptor::new(
                BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(100, 199).unwrap()),
                BTreeSet::from([api]),
            )]
    );
}

#[test]
fn block_index_blocks_exposes_inserted_descriptors_in_key_order() {
    let mut blocks = BlockIndex::default();
    let first = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([1]),
    );
    let second = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 21, 30, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([2]),
    );

    blocks.insert(second.clone());
    blocks.insert(first.clone());

    assert2::assert!(blocks.blocks() == &[first, second]);
}

#[test]
fn deterministic_block_keys_encode_compactor_idempotency_fields() {
    let key = BlockKey::new("tenant-a", 2, 42, 99, TimeRange::new(1_000, 2_000).unwrap());

    assert2::assert!(
        key.object_key() == "tenant=tenant-a/partition=2/offsets=42-99/time=1000-2000.parquet"
    );
}
