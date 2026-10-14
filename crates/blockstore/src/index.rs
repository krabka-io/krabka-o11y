//! Label, series and block index, in memory and on object storage.
//!
//! The in-memory side resolves matchers to series through posting lists and
//! then series and a time range to the blocks worth reading. The on-storage
//! side cuts the same structure into one object per tenant per slice of time,
//! so a query fetches and holds the slices it asked about rather than the
//! whole fleet. See [`Index`] for the layout and what it costs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::StreamExt;
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub use crate::labels::LabelPair;
use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    compaction::BlockLevel,
    error::{BlockStoreError, Result},
    labels::{Labels, SeriesFingerprint},
    matcher::{LabelMatcher, MatchOp, QUERY_SHARD_LABEL, parse_query_shard_selector},
    path_escape::{escape_object_path_segment, unescape_object_path_segment},
    shard_string_dictionary::assign_sorted_string_ids,
};

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use object_store::{ObjectStore, path::Path};

    use super::*;
    use crate::{
        awkward_tenants::AWKWARD_TENANTS,
        block::BlockMeta,
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    struct SavedIndex {
        store: Arc<dyn ObjectStore>,
        key: String,
    }

    struct ResolveCase {
        name: &'static str,
        tenant: &'static str,
        matchers: Vec<LabelMatcher>,
        expected: BTreeSet<SeriesFingerprint>,
    }

    struct MatcherResultCase {
        name: &'static str,
        matcher: LabelMatcher,
        expected: BTreeSet<SeriesFingerprint>,
    }

    struct CandidateTimeCase {
        name: &'static str,
        min_ts: i64,
        max_ts: i64,
        expected: Vec<String>,
    }

    struct MatcherValidityCase {
        name: &'static str,
        matchers: Vec<LabelMatcher>,
        expected_ok: bool,
    }

    struct MatcherListResultCase {
        name: &'static str,
        matchers: Vec<LabelMatcher>,
        expected: BTreeSet<SeriesFingerprint>,
    }

    struct ShardSelectorCase {
        op: MatchOp,
        keep_remainder: u64,
    }

    struct LabelValuesQueryCase {
        name: &'static str,
        tenant: &'static str,
        matchers: Vec<LabelMatcher>,
        expected: Vec<String>,
    }

    struct BlockBoundsCase {
        name: &'static str,
        tenant: &'static str,
        min_ts: i64,
        max_ts: i64,
        expected: Option<crate::block::BlockTimeBounds>,
    }

    struct BoundsOverlapCase {
        name: &'static str,
        min_ts: i64,
        max_ts: i64,
        expected: Option<crate::block::BlockTimeBounds>,
    }

    fn labels(pairs: &[LabelPair]) -> Labels {
        Labels::from_pairs(pairs.iter().cloned())
    }

    fn seed() -> Index {
        let mut idx = Index::new();
        let api_prod = labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]);
        let api_dev = labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]);
        let web_prod = labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]);
        idx.add_series("t", api_prod.fingerprint(), &api_prod);
        idx.add_series("t", api_dev.fingerprint(), &api_dev);
        idx.add_series("t", web_prod.fingerprint(), &web_prod);
        idx
    }

    fn add_test_block(
        index: &mut Index,
        object_key: &str,
        min_ts: i64,
        max_ts: i64,
        fingerprints: Vec<SeriesFingerprint>,
    ) {
        index.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: object_key.into(),
            min_ts,
            max_ts,
            row_count: 1,
            fingerprints,
            level: BlockLevel::INGESTED,
        });
    }

    /// The fingerprints of the three series that [`seed`] indexes.
    struct SeededFingerprints {
        api_prod: SeriesFingerprint,
        api_dev: SeriesFingerprint,
        web_prod: SeriesFingerprint,
    }

    impl SeededFingerprints {
        fn new() -> Self {
            Self {
                api_prod: labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
                    .fingerprint(),
                api_dev: labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")])
                    .fingerprint(),
                web_prod: labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")])
                    .fingerprint(),
            }
        }
    }

    #[test]
    fn snapshot_size_cap_is_256_mib() {
        assert2::assert!(MAX_INDEX_SNAPSHOT_BYTES == mebibytes(256));
        assert2::assert!(MAX_INDEX_SNAPSHOT_BYTES.bytes_u64() == 256 * 1024 * 1024);
    }

    #[test]
    fn series_storage_preserves_json_and_shard_bytes() {
        let mut index = Index::new();
        index.add_series("t", 7, &labels(&[LabelPair::new("a", "b")]));
        // Independent v2 ledger: tenant, sorted dictionary, stored fingerprint,
        // one label pair, no blocks and no block postings.
        assert2::assert!(
            index.encode_tenant_as_shard("t")
                == b"KBIX\x02\x01t\x02\x01a\x01b\x01\x07\0\0\0\0\0\0\0\x01\0\x01\0\0"
        );
        let json = serde_json::to_value(&index).unwrap();
        assert2::assert!(json["tenants"]["t"]["series"] == serde_json::json!({"7": {"a": "b"}}));
        let restored: Index = serde_json::from_value(json).unwrap();
        assert2::assert!(
            restored.series("t", &[]).unwrap() == vec![labels(&[LabelPair::new("a", "b")])]
        );
        let decoded = decode_index_shard(
            "golden",
            b"KBIX\x02\x01t\x02\x01a\x01b\x01\x07\0\0\0\0\0\0\0\x01\0\x01\0\0",
        )
        .unwrap();
        assert2::assert!(
            decoded.series("t", &[]).unwrap() == vec![labels(&[LabelPair::new("a", "b")])]
        );
        assert2::assert!(
            decoded
                .resolve("t", &[LabelMatcher::new("a", MatchOp::Eq, "b")])
                .unwrap()
                == BTreeSet::from([7])
        );
    }

    #[test]
    fn series_keep_tenant_order_first_labels_and_independent_snapshots() {
        let mut index = Index::new();
        let api = labels(&[
            LabelPair::new("app", "api"),
            LabelPair::new("region", "東京"),
        ]);
        let worker = labels(&[
            LabelPair::new("app", "worker"),
            LabelPair::new("region", "west"),
        ]);
        index.add_series("t", 9, &worker);
        index.add_series("t", 7, &api);
        index.add_series("other", 7, &api);
        let snapshot = index.clone();
        let held = snapshot.series("t", &[]).unwrap();
        assert2::assert!(held == vec![api.clone(), worker.clone()]);
        index.add_series("t", 7, &labels(&[LabelPair::new("app", "replacement")]));
        index.add_series("t", 11, &labels(&[LabelPair::new("app", "new")]));
        let matcher = [LabelMatcher::new("app", MatchOp::Eq, "api")];
        let current = index.series("t", &matcher).unwrap();
        let other = index.series("other", &matcher).unwrap();
        assert2::assert!(current == vec![api.clone()]);
        assert2::assert!(other == vec![api.clone()]);
        assert2::assert!(index.series("missing", &[]).unwrap().is_empty());
        assert2::assert!(snapshot.series("t", &[]).unwrap() == vec![api.clone(), worker.clone()]);
        let mut owned = index.series("t", &matcher).unwrap();
        owned[0].insert("app", "mutated");
        assert2::assert!(current == vec![api.clone()]);
        assert2::assert!(index.series("t", &matcher).unwrap() == vec![api.clone()]);
        drop(index);
        drop(snapshot);
        assert2::assert!(held == vec![api, worker]);
    }

    #[test]
    fn resolve_matcher_cases() {
        let idx = seed();
        let SeededFingerprints {
            api_prod,
            api_dev,
            web_prod,
        } = SeededFingerprints::new();
        for case in [
            ResolveCase {
                name: "equal intersection",
                tenant: "t",
                matchers: vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Eq, "prod"),
                ],
                expected: BTreeSet::from([api_prod]),
            },
            ResolveCase {
                name: "not equal exclusion",
                tenant: "t",
                matchers: vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "prod"),
                ],
                expected: BTreeSet::from([api_dev]),
            },
            ResolveCase {
                name: "regex union",
                tenant: "t",
                matchers: vec![LabelMatcher::new("env", MatchOp::Re, "pro.*")],
                expected: BTreeSet::from([api_prod, web_prod]),
            },
            ResolveCase {
                name: "unknown tenant",
                tenant: "nope",
                matchers: vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                expected: BTreeSet::new(),
            },
        ] {
            assert2::assert!(
                idx.resolve(case.tenant, &case.matchers).unwrap() == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn eq_does_not_collide_across_nul_boundary() {
        // `LabelPair::new("x", "a\0b")` and `LabelPair::new("x\0a", "b")` share the same naive
        // `name\0value` byte string, so an in-band NUL delimiter would index
        // both under one bucket and contaminate Eq results across series.
        let mut idx = Index::new();
        let s1 = labels(&[LabelPair::new("x", "a\u{0}b")]);
        let s2 = labels(&[LabelPair::new("x\u{0}a", "b")]);
        idx.add_series("t", s1.fingerprint(), &s1);
        idx.add_series("t", s2.fingerprint(), &s2);

        for case in [
            MatcherResultCase {
                name: "NUL in label value",
                matcher: LabelMatcher::new("x", MatchOp::Eq, "a\u{0}b"),
                expected: BTreeSet::from([s1.fingerprint()]),
            },
            MatcherResultCase {
                name: "NUL in label name",
                matcher: LabelMatcher::new("x\u{0}a", MatchOp::Eq, "b"),
                expected: BTreeSet::from([s2.fingerprint()]),
            },
        ] {
            assert2::assert!(
                idx.resolve("t", &[case.matcher]).unwrap() == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn candidate_blocks_prune_by_fp_and_time() {
        let mut idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let web_prod =
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]).fingerprint();
        add_test_block(&mut idx, "b1.parquet", 0, 100, vec![api_prod]);
        add_test_block(&mut idx, "b2.parquet", 200, 300, vec![web_prod]);

        for case in [
            CandidateTimeCase {
                name: "matching fingerprint and time",
                min_ts: 0,
                max_ts: 150,
                expected: vec!["b1.parquet".to_string()],
            },
            CandidateTimeCase {
                name: "outside time range",
                min_ts: 500,
                max_ts: 600,
                expected: Vec::new(),
            },
        ] {
            assert2::assert!(
                idx.candidate_blocks("t", &BTreeSet::from([api_prod]), case.min_ts, case.max_ts)
                    == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn label_names_and_values() {
        let idx = seed();
        assert2::assert!(idx.label_names("t") == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(
            idx.label_values("t", "env") == vec!["dev".to_string(), "prod".to_string()]
        );
    }

    #[test]
    fn invalid_regex_returns_err() {
        let idx = seed();

        let got = idx.resolve("t", &[LabelMatcher::new("env", MatchOp::Re, "[")]);

        assert2::assert!(got.is_err());
    }

    #[test]
    fn empty_matchers_returns_err() {
        let idx = seed();

        let got = idx.resolve("t", &[]);

        assert2::assert!(got.is_err());
    }

    #[test]
    fn all_empty_matching_selector_returns_err() {
        let idx = seed();

        // Every matcher below matches the empty (absent) value, so the selector
        // restricts nothing and would force a full tenant scan; Prometheus
        // rejects it. Each is tested as the sole matcher in the selector.
        let cases = [
            MatcherValidityCase {
                name: "not-equal matcher accepts empty",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Neq, "bar")],
                expected_ok: false,
            },
            MatcherValidityCase {
                name: "equal-empty matcher",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Eq, "")],
                expected_ok: false,
            },
            MatcherValidityCase {
                name: "match-all regex",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Re, ".*")],
                expected_ok: false,
            },
            MatcherValidityCase {
                name: "negative regex accepts empty",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Nre, "bar")],
                expected_ok: false,
            },
            MatcherValidityCase {
                name: "synthetic shard only",
                matchers: vec![LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2")],
                expected_ok: false,
            },
            MatcherValidityCase {
                name: "restricting regex",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Re, ".*bar.*")],
                expected_ok: true,
            },
            MatcherValidityCase {
                name: "non-empty regex",
                matchers: vec![LabelMatcher::new("foo", MatchOp::Re, ".+")],
                expected_ok: true,
            },
            MatcherValidityCase {
                name: "empty matcher paired with restricting matcher",
                matchers: vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "dev"),
                ],
                expected_ok: true,
            },
        ];
        for case in cases {
            assert2::assert!(
                idx.resolve("t", &case.matchers).is_ok() == case.expected_ok,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn tenant_isolation_for_same_labels() {
        let mut idx = Index::new();
        let tenant_a = labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]);
        let tenant_b = labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]);
        let other = labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]);
        idx.add_series("a", tenant_a.fingerprint(), &tenant_a);
        idx.add_series("b", tenant_b.fingerprint(), &tenant_b);
        idx.add_series("b", other.fingerprint(), &other);

        let got = idx
            .resolve("a", &[LabelMatcher::new("env", MatchOp::Eq, "prod")])
            .unwrap();

        assert2::assert!(got == BTreeSet::from([tenant_a.fingerprint()]));
    }

    #[test]
    fn add_series_is_idempotent_for_existing_fingerprint() {
        let mut idx = Index::new();
        let original = labels(&[LabelPair::new("app", "api")]);
        let replacement = labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]);
        let fp = original.fingerprint();
        idx.add_series("t", fp, &original);
        idx.add_series("t", fp, &replacement);

        let snapshot = serde_json::to_string(&idx).unwrap();
        assert2::assert!(idx.label_names("t") == vec!["app".to_string()]);
        assert2::assert!(
            idx.resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
                .unwrap()
                == BTreeSet::from([fp])
        );
        assert2::assert!(!snapshot.contains("web"));
        assert2::assert!(!snapshot.contains("env"));
    }

    #[test]
    fn absent_labels_match_empty_string_semantics() {
        let idx = seed();
        let SeededFingerprints {
            api_prod,
            api_dev,
            web_prod,
        } = SeededFingerprints::new();
        let all = BTreeSet::from([api_prod, api_dev, web_prod]);

        // The empty-string matchers below all match the absent label, so each is
        // anchored with a non-empty `app=~".+"` matcher (which selects every
        // seeded series) to form a valid Prometheus vector selector; the anchor
        // does not change the empty-string posting result under test.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for case in [
            MatcherResultCase {
                name: "equal empty",
                matcher: LabelMatcher::new("missing", MatchOp::Eq, ""),
                expected: all.clone(),
            },
            MatcherResultCase {
                name: "regex empty",
                matcher: LabelMatcher::new("missing", MatchOp::Re, ".*"),
                expected: all.clone(),
            },
            MatcherResultCase {
                name: "not equal empty",
                matcher: LabelMatcher::new("missing", MatchOp::Neq, ""),
                expected: BTreeSet::new(),
            },
            MatcherResultCase {
                name: "not regex empty",
                matcher: LabelMatcher::new("missing", MatchOp::Nre, ".*"),
                expected: BTreeSet::new(),
            },
        ] {
            assert2::assert!(
                idx.resolve("t", &[anchor.clone(), case.matcher]).unwrap() == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn present_empty_labels_match_empty_string_semantics() {
        let mut idx = Index::new();
        let empty_zone = labels(&[LabelPair::new("app", "api"), LabelPair::new("zone", "")]);
        let absent_zone = labels(&[LabelPair::new("app", "web")]);
        let non_empty_zone = labels(&[LabelPair::new("app", "db"), LabelPair::new("zone", "us")]);
        idx.add_series("t", empty_zone.fingerprint(), &empty_zone);
        idx.add_series("t", absent_zone.fingerprint(), &absent_zone);
        idx.add_series("t", non_empty_zone.fingerprint(), &non_empty_zone);
        let empty_equivalent =
            BTreeSet::from([empty_zone.fingerprint(), absent_zone.fingerprint()]);

        // `zone=""` matches the empty string, so anchor with a non-empty matcher
        // (`app=~".+"` selects all three series) to form a valid selector.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for case in [
            MatcherListResultCase {
                name: "equal empty",
                matchers: vec![anchor.clone(), LabelMatcher::new("zone", MatchOp::Eq, "")],
                expected: empty_equivalent,
            },
            MatcherListResultCase {
                name: "not equal empty",
                matchers: vec![LabelMatcher::new("zone", MatchOp::Neq, "")],
                expected: BTreeSet::from([non_empty_zone.fingerprint()]),
            },
        ] {
            assert2::assert!(
                idx.resolve("t", &case.matchers).unwrap() == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn resolve_query_shard_selects_by_series_fingerprint_modulo() {
        let mut idx = Index::new();
        let series = (0..12)
            .map(|id| {
                labels(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("series", id.to_string()),
                ])
            })
            .collect::<Vec<_>>();
        for labels in &series {
            idx.add_series("t", labels.fingerprint(), labels);
        }

        // `=` keeps the shard's own series; `!=` returns the complement.
        for case in [
            ShardSelectorCase {
                op: MatchOp::Eq,
                keep_remainder: 0,
            },
            ShardSelectorCase {
                op: MatchOp::Neq,
                keep_remainder: 1,
            },
        ] {
            let expected = series
                .iter()
                .map(Labels::fingerprint)
                .filter(|fp| fp % 2 == case.keep_remainder)
                .collect::<BTreeSet<_>>();
            let got = idx
                .resolve(
                    "t",
                    &[
                        LabelMatcher::new("app", MatchOp::Eq, "api"),
                        LabelMatcher::new("__query_shard__", case.op, "1_of_2"),
                    ],
                )
                .unwrap();

            assert2::assert!(!expected.is_empty());
            assert2::assert!(expected.len() < series.len());
            assert2::assert!(got == expected);
        }
    }

    #[test]
    fn matching_fingerprints_returns_matched_set() {
        let idx = seed();
        let SeededFingerprints {
            api_prod,
            api_dev,
            web_prod,
        } = SeededFingerprints::new();
        for case in [
            ResolveCase {
                name: "specific matcher",
                tenant: "t",
                matchers: vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                expected: BTreeSet::from([api_prod, api_dev]),
            },
            ResolveCase {
                name: "all tenant series",
                tenant: "t",
                matchers: Vec::new(),
                expected: BTreeSet::from([api_prod, api_dev, web_prod]),
            },
            ResolveCase {
                name: "unknown tenant",
                tenant: "nope",
                matchers: Vec::new(),
                expected: BTreeSet::new(),
            },
        ] {
            assert2::assert!(
                idx.matching_fingerprints(case.tenant, &case.matchers)
                    .unwrap()
                    == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn label_names_for_returns_distinct_sorted_names() {
        let idx = seed();
        let names = idx
            .label_names_for("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(names == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(idx.label_names_for("nope", &[]).unwrap().is_empty());
    }

    #[test]
    fn label_names_for_fingerprints_returns_distinct_names() {
        let idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let names = idx.label_names_for_fingerprints("t", &BTreeSet::from([api_prod]));
        assert2::assert!(names == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(
            idx.label_names_for_fingerprints("nope", &BTreeSet::from([api_prod]))
                .is_empty()
        );
    }

    #[test]
    fn label_values_for_returns_distinct_sorted_values() {
        let idx = seed();
        for case in [
            LabelValuesQueryCase {
                name: "all api environments",
                tenant: "t",
                matchers: vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                expected: vec!["dev".to_string(), "prod".to_string()],
            },
            LabelValuesQueryCase {
                name: "only web environment",
                tenant: "t",
                matchers: vec![LabelMatcher::new("app", MatchOp::Eq, "web")],
                expected: vec!["prod".to_string()],
            },
            LabelValuesQueryCase {
                name: "unknown tenant",
                tenant: "nope",
                matchers: Vec::new(),
                expected: Vec::new(),
            },
        ] {
            assert2::assert!(
                idx.label_values_for(case.tenant, "env", &case.matchers)
                    .unwrap()
                    == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn label_values_for_fingerprints_returns_distinct_values() {
        let idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let api_dev =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]).fingerprint();
        let values =
            idx.label_values_for_fingerprints("t", "env", &BTreeSet::from([api_prod, api_dev]));
        assert2::assert!(values == vec!["dev".to_string(), "prod".to_string()]);
        assert2::assert!(
            idx.label_values_for_fingerprints("nope", "env", &BTreeSet::from([api_prod]))
                .is_empty()
        );
    }

    #[test]
    fn series_projects_requested_label_names() {
        let idx = seed();
        let got = idx
            .series_projected(
                "t",
                &[LabelMatcher::new("app", MatchOp::Eq, "api")],
                &["app".to_string(), "env".to_string()],
            )
            .unwrap();
        assert2::assert!(
            got == vec![
                vec![LabelPair::new("app", "api"), LabelPair::new("env", "dev")],
                vec![LabelPair::new("app", "api"), LabelPair::new("env", "prod")],
            ]
        );
        assert2::assert!(
            idx.series_projected("nope", &[], &["app".to_string()])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn series_returns_full_label_sets() {
        let idx = seed();
        let got = idx
            .series("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(
            got == vec![
                labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
                labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]),
            ]
        );

        let mut expected_all = vec![
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]),
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]),
        ];
        expected_all.sort_by_key(Labels::fingerprint);
        assert2::assert!(idx.series("t", &[]).unwrap() == expected_all);
        assert2::assert!(idx.series("nope", &[]).unwrap() == Vec::new());
    }

    #[test]
    fn series_for_fingerprints_projects_label_values() {
        let idx = seed();
        let web_prod =
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["app".to_string(), "env".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                LabelPair::new("app", "web"),
                LabelPair::new("env", "prod"),
            ]]
        );
        assert2::assert!(
            idx.series_for_fingerprints("nope", &BTreeSet::from([web_prod]), &["app".to_string()])
                .is_empty()
        );
    }

    #[test]
    fn series_for_fingerprints_projection_is_sorted_by_name() {
        // Pyroscope's `/series` emits each set's labels SORTED by name regardless
        // of the request's `labelNames` order. Request the projection in REVERSE
        // sorted order (`env` before `app`) and assert the response is still
        // `[app, env]` — the wire order the Grafana drilldown compares against.
        let idx = seed();
        let web_prod =
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["env".to_string(), "app".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                LabelPair::new("app", "web"),
                LabelPair::new("env", "prod"),
            ]]
        );
    }

    #[test]
    fn series_for_fingerprints_empty_names_returns_full_label_sets() {
        // Empty `label_names` means "return all labels" (the
        // Prometheus/Loki/Pyroscope `/series` convention). Previously this
        // returned a single empty label set (`[{}]`), breaking Grafana's
        // Pyroscope label autocomplete.
        let idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let api_dev =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]).fingerprint();
        let mut got = idx.series_for_fingerprints("t", &BTreeSet::from([api_prod, api_dev]), &[]);
        got.sort();
        assert2::assert!(
            got == vec![
                vec![LabelPair::new("app", "api"), LabelPair::new("env", "dev"),],
                vec![LabelPair::new("app", "api"), LabelPair::new("env", "prod"),],
            ]
        );
    }

    #[test]
    fn candidate_blocks_for_series_returns_pruned_keys() {
        let mut idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        add_test_block(&mut idx, "b1.parquet", 0, 100, vec![api_prod]);
        let got = idx.candidate_blocks_for_series("t", &BTreeSet::from([api_prod]), 0, 150);
        assert2::assert!(got == vec!["b1.parquet".to_string()]);
    }

    #[test]
    fn block_time_bounds_spans_overlapping_blocks() {
        let mut idx = seed();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 10,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![],
            level: BlockLevel::INGESTED,
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 350,
            row_count: 1,
            fingerprints: vec![],
            level: BlockLevel::INGESTED,
        });

        for case in [
            // Window covering both → combined min/max across them.
            BlockBoundsCase {
                name: "both blocks",
                tenant: "t",
                min_ts: 0,
                max_ts: 1_000,
                expected: Some(crate::block::BlockTimeBounds {
                    min_ts: 10,
                    max_ts: 350,
                }),
            },
            // Window covering only b1 → exactly b1's bounds (kills Some((x,y)) stubs).
            BlockBoundsCase {
                name: "first block",
                tenant: "t",
                min_ts: 0,
                max_ts: 150,
                expected: Some(crate::block::BlockTimeBounds {
                    min_ts: 10,
                    max_ts: 100,
                }),
            },
            // Window that overlaps nothing → None.
            BlockBoundsCase {
                name: "no overlap",
                tenant: "t",
                min_ts: 500,
                max_ts: 600,
                expected: None,
            },
            // Unknown tenant → None.
            BlockBoundsCase {
                name: "unknown tenant",
                tenant: "nope",
                min_ts: 0,
                max_ts: 1_000,
                expected: None,
            },
        ] {
            assert2::assert!(
                idx.block_time_bounds(case.tenant, case.min_ts, case.max_ts) == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn block_time_bounds_overlap_filter_is_inclusive_on_both_ends() {
        let mut idx = Index::new();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b.parquet".into(),
            min_ts: 100,
            max_ts: 200,
            row_count: 1,
            fingerprints: vec![],
            level: BlockLevel::INGESTED,
        });

        for case in [
            // Touch the block's max at the window's min: b.min_ts(100) <= max_ts(200)
            // && b.max_ts(200) >= min_ts(200). `<=`→`>` or `>=`→`<` would drop it.
            BoundsOverlapCase {
                name: "touches maximum",
                min_ts: 200,
                max_ts: 300,
                expected: Some(crate::block::BlockTimeBounds {
                    min_ts: 100,
                    max_ts: 200,
                }),
            },
            // Touch the block's min at the window's max.
            BoundsOverlapCase {
                name: "touches minimum",
                min_ts: 0,
                max_ts: 100,
                expected: Some(crate::block::BlockTimeBounds {
                    min_ts: 100,
                    max_ts: 200,
                }),
            },
            // A window entirely above the block: with `&&`→`||` this would wrongly
            // include the block (one side still true), so demand None here.
            BoundsOverlapCase {
                name: "entirely above",
                min_ts: 300,
                max_ts: 400,
                expected: None,
            },
            // A window entirely below the block: the other side is the true one.
            BoundsOverlapCase {
                name: "entirely below",
                min_ts: 0,
                max_ts: 50,
                expected: None,
            },
        ] {
            assert2::assert!(
                idx.block_time_bounds("t", case.min_ts, case.max_ts) == case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn all_blocks_lists_every_registered_block() {
        let mut idx = seed();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 7,
            fingerprints: vec![],
            level: BlockLevel::INGESTED,
        });
        idx.add_block(&BlockMeta {
            tenant: "u".into(),
            object_key: "b2.parquet".into(),
            min_ts: 5,
            max_ts: 9,
            row_count: 3,
            fingerprints: vec![],
            level: BlockLevel::INGESTED,
        });

        let mut blocks = idx.all_blocks_unscoped();
        blocks.sort_by(|a, b| a.object_key.cmp(&b.object_key));
        assert2::assert!(
            blocks
                == vec![
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b1.parquet".to_string(),
                        min_ts: 0,
                        max_ts: 100,
                        row_count: 7,
                        fingerprints: vec![],
                        level: BlockLevel::INGESTED,
                    },
                    BlockMeta {
                        tenant: "u".to_string(),
                        object_key: "b2.parquet".to_string(),
                        min_ts: 5,
                        max_ts: 9,
                        row_count: 3,
                        fingerprints: vec![],
                        level: BlockLevel::INGESTED,
                    },
                ]
        );

        // Tenant-scoped `all_blocks` returns only that tenant's blocks.
        assert2::assert!(
            idx.all_blocks("t")
                == vec![BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "b1.parquet".to_string(),
                    min_ts: 0,
                    max_ts: 100,
                    row_count: 7,
                    fingerprints: vec![],
                    level: BlockLevel::INGESTED,
                }]
        );
    }

    #[test]
    fn resolve_nre_excludes_regex_matches() {
        // `Nre` negates the regex match set: the `all_fingerprints().difference`
        // against the matches. Deleting the negation would flip it to keep only
        // the matches. A bare `{env!~"pro.*"}` is rejected by the non-empty
        // matcher gate (it matches absent `env`, exactly Prometheus' rule), so
        // anchor it with `app=~".+"`, which matches all three seed series and
        // therefore leaves the negated `env` match as the sole discriminator.
        let idx = seed();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Re, ".+"),
                    LabelMatcher::new("env", MatchOp::Nre, "pro.*"),
                ],
            )
            .unwrap();
        // Only the `env=dev` series survives the negated `pro.*` match.
        let api_dev =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]).fingerprint();
        assert2::assert!(got == BTreeSet::from([api_dev]));
    }

    #[test]
    fn index_implements_block_index_time_prefilter() {
        let mut idx = seed();
        <Index as BlockIndex>::add_block(
            &mut idx,
            &BlockMeta {
                tenant: "t".into(),
                object_key: "b1.parquet".into(),
                min_ts: 0,
                max_ts: 100,
                row_count: 1,
                fingerprints: vec![],
                level: BlockLevel::INGESTED,
            },
        );
        <Index as BlockIndex>::add_block(
            &mut idx,
            &BlockMeta {
                tenant: "t".into(),
                object_key: "b2.parquet".into(),
                min_ts: 200,
                max_ts: 300,
                row_count: 1,
                fingerprints: vec![],
                level: BlockLevel::INGESTED,
            },
        );

        assert2::assert!(<Index as BlockIndex>::block_count(&idx, "t") == 2);
        assert2::assert!(
            <Index as BlockIndex>::candidate_blocks(&idx, "t", 50, 150)
                == vec!["b1.parquet".to_string()]
        );
    }

    #[test]
    fn add_block_is_idempotent_by_object_key() {
        let mut idx = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let meta = BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
            level: BlockLevel::INGESTED,
        };

        idx.add_block(&meta);
        idx.add_block(&meta);

        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 100);
        assert2::assert!(got == vec!["b1.parquet".to_string()]);
    }

    /// The shard bytes are the only copy of a block's level, so an encoding
    /// that dropped it would restart the compaction ladder at zero on every
    /// save and the planner would rewrite the same rows for as long as it ran.
    #[tokio::test]
    async fn a_block_level_survives_the_shard_encoding() {
        use object_store::memory::InMemory;

        let compacted = BlockMeta {
            tenant: "t".into(),
            object_key: "c1.parquet".into(),
            min_ts: 0,
            max_ts: 50,
            row_count: 9,
            fingerprints: vec![
                labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
                    .fingerprint(),
            ],
            level: BlockLevel(3),
        };
        let mut index = seed();
        index.add_block(&compacted);

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index
            .save_with_shard_width(&store, "index/metrics.json", 100)
            .await
            .unwrap();
        let loaded = Index::load(&store, "index/metrics.json").await.unwrap();

        assert2::assert!(loaded.all_blocks_unscoped() == vec![compacted]);
        assert2::assert!(loaded.block_level("c1.parquet") == Some(BlockLevel(3)));
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;
        let before = Index::load(store, key).await.unwrap();

        let loaded = Index::load(store, key).await.unwrap();
        let got = loaded
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();

        assert2::assert!(
            got == BTreeSet::from([
                labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
                    .fingerprint(),
                labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")]).fingerprint(),
            ])
        );

        // Every field of every block survives, fingerprints included: the
        // shard encoding is the only copy of them now, and a block that comes
        // back with the wrong span or the wrong series is a block the pruner
        // will offer to the wrong query.
        assert2::assert!(
            before.all_blocks_unscoped()
                == vec![
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b1.parquet".to_string(),
                        min_ts: 0,
                        max_ts: 50,
                        row_count: 1,
                        fingerprints: vec![
                            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
                                .fingerprint()
                        ],
                        level: BlockLevel::INGESTED,
                    },
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b2.parquet".to_string(),
                        min_ts: 1_000,
                        max_ts: 1_050,
                        row_count: 1,
                        fingerprints: vec![
                            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")])
                                .fingerprint()
                        ],
                        level: BlockLevel::INGESTED,
                    },
                ]
        );

        // And a second save from the loaded copy is the same index again,
        // which is what a writer restarting from storage does.
        loaded.save_with_shard_width(store, key, 100).await.unwrap();
        let again = Index::load(store, key).await.unwrap();
        assert2::assert!(again.all_blocks_unscoped() == before.all_blocks_unscoped());
        assert2::assert!(again.label_names("t") == before.label_names("t"));
    }

    #[tokio::test]
    async fn load_rejects_a_shard_over_the_cap() {
        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;

        // The cap is per shard now that an index is many objects, so the
        // rejection has to name the shard and its own size, not the index.
        let shard = one_shard_object(store).await;
        let size = store.head(&shard).await.unwrap().size;
        assert2::assert!(size > 1);

        let got = Index::load_with_cap(store, key, bytes(1)).await;
        let Err(BlockStoreError::InvalidBlock(msg)) = got else {
            panic!("expected InvalidBlock for an oversized index shard");
        };
        assert2::assert!(
            msg == format!("index shard `{shard}` is {size} bytes, exceeds cap of 1 bytes")
        );

        // A cap at or above every shard still loads.
        let loaded = Index::load_with_cap(store, key, mebibytes(1))
            .await
            .unwrap();
        assert2::assert!(loaded.block_count("t") == 2);
    }

    #[tokio::test]
    async fn loading_an_index_that_was_never_saved_yields_an_empty_index() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory};

        // An index with no shards is a key nothing has been written under yet,
        // which is a first save that has not happened rather than a failure.
        // The single-object loader reported the store's own not-found here.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let whole = Index::load(&store, "index/missing.json").await.unwrap();
        let ranged = Index::load_for_range(&store, "index/missing.json", "t", 0, 100)
            .await
            .unwrap();

        assert2::assert!(whole.block_count("t") == 0);
        assert2::assert!(ranged.block_count("t") == 0);
        assert2::assert!(whole.label_names("t").is_empty());
    }

    #[tokio::test]
    async fn a_range_load_does_not_read_the_shards_outside_the_range() {
        use object_store::{PutPayload, path::Path};

        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;

        // Corrupting the far shard is how "did not read it" is observed
        // without a mock store: a loader that touched the object would fail to
        // decode it, and one that pruned it by the span in its key never sees
        // the bytes.
        let far = Path::from(index_shard_object_key(
            key,
            "t",
            IndexShardRange::new(1_000, 1_099),
        ));
        store
            .put(&far, PutPayload::from_static(b"not an index shard"))
            .await
            .unwrap();

        let near = Index::load_for_range(store, key, "t", 0, 60).await.unwrap();
        assert2::assert!(near.blocks_in_range("t", 0, 2_000) == vec!["b1.parquet".to_string()]);

        // The same load over the whole span does read it, and says so.
        let whole = Index::load_for_range(store, key, "t", 0, 2_000).await;
        let Err(BlockStoreError::InvalidBlock(msg)) = whole else {
            panic!("expected the corrupt shard to be reported");
        };
        assert2::assert!(msg == format!("index shard `{far}`: is not an index shard"));
    }

    #[tokio::test]
    async fn a_range_load_carries_the_series_of_the_shards_it_read() {
        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();

        let loaded = Index::load_for_range(store, key, "t", 0, 60).await.unwrap();

        // The labels, the postings rebuilt from them, and the block-to-series
        // pairs all survive the shard the range selected.
        assert2::assert!(
            loaded
                .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
                .unwrap()
                == BTreeSet::from([
                    api_prod,
                    labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "dev")])
                        .fingerprint(),
                ])
        );
        assert2::assert!(
            loaded.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 60)
                == vec!["b1.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn a_series_no_block_carries_yet_survives_a_range_load() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory};

        // `seed` registers three series and no blocks at all, which is what a
        // writer looks like before its first flush. Sharding is cut on block
        // spans, so such a series belongs to no shard and would be dropped by
        // a layout that only wrote shards.
        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index.save(&store, "index/metrics.json").await.unwrap();

        let loaded = Index::load_for_range(&store, "index/metrics.json", "t", 500, 600)
            .await
            .unwrap();

        assert2::assert!(
            loaded.label_values("t", "env") == vec!["dev".to_string(), "prod".to_string()]
        );
    }

    #[tokio::test]
    async fn a_re_save_deletes_the_shards_the_new_layout_does_not_name() {
        use object_store::path::Path;

        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;
        let before = shard_object_keys(store).await;
        assert2::assert!(before.len() == 2);

        // Drop the later block and save again. The shard that held it is no
        // longer part of the index, and leaving it behind would make the next
        // whole-index load resurrect the block.
        let mut index = Index::load(store, key).await.unwrap();
        index.replace_blocks("t", &["b2.parquet".to_string()], &[]);
        index.save_with_shard_width(store, key, 100).await.unwrap();

        let after = shard_object_keys(store).await;
        assert2::assert!(
            after
                == vec![Path::from(index_shard_object_key(
                    key,
                    "t",
                    IndexShardRange::new(0, 99),
                ))]
        );
        let reloaded = Index::load(store, key).await.unwrap();
        assert2::assert!(reloaded.blocks_in_range("t", 0, 2_000) == vec!["b1.parquet".to_string()]);
    }

    #[tokio::test]
    async fn the_shard_encoding_is_far_smaller_than_the_json_it_replaced() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory};

        // Ten thousand series over four blocks, each block holding all of them
        // — the shape a scrape loop produces, and the shape whose JSON was
        // dominated by the same fingerprints written out in decimal once per
        // block and once per posting list.
        let mut index = Index::new();
        let mut fingerprints = Vec::new();
        for series in 0..10_000_u64 {
            let labels = Labels::from_pairs([
                LabelPair::new("__name__", "http_requests_total"),
                LabelPair::new("job", format!("job-{}", series % 16)),
                LabelPair::new("pod", format!("pod-{series}")),
            ]);
            fingerprints.push(labels.fingerprint());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        for block in 0..4_i64 {
            index.add_block(&BlockMeta {
                tenant: "t".into(),
                object_key: format!("blocks/{block}.parquet"),
                min_ts: block * 100,
                max_ts: block * 100 + 99,
                row_count: 10_000,
                fingerprints: fingerprints.clone(),
                level: BlockLevel::INGESTED,
            });
        }

        // One shard, so the comparison is encoding against encoding rather
        // than a whole document against a slice of one.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index
            .save_with_shard_width(&store, "index/metrics.json", 100_000)
            .await
            .unwrap();
        let whole = shard_object_keys(&store).await;
        assert2::assert!(whole.len() == 1);
        let binary = store.head(&whole[0]).await.unwrap().size;
        let json = u64::try_from(serde_json::to_vec(&index).unwrap().len()).unwrap();

        // Cut into a shard per block, a query for one block reads one of them.
        index
            .save_with_shard_width(&store, "index/metrics.json", 100)
            .await
            .unwrap();
        let shards = shard_object_keys(&store).await;
        assert2::assert!(shards.len() == 4);
        let mut one_shard = 0_u64;
        for key in &shards {
            one_shard = one_shard.max(store.head(key).await.unwrap().size);
        }

        // The dictionary and the varints against the same index as JSON: the
        // run at this size is a factor of five.
        assert2::assert!(json > binary * 3);
        // And what a query over one block's worth of time has to read.
        assert2::assert!(json > one_shard * 5);
    }

    #[test]
    fn shard_bounds_sort_the_way_the_timestamps_do() {
        // Shard spans travel in object keys, and a listing is lexicographic.
        // A bound spelled as a bare `i64` would put every negative timestamp
        // after every positive one and every short number before a longer one.
        let bounds = [i64::MIN, -86_400_000, -1, 0, 1, 86_400_000, i64::MAX];
        for index in 0..bounds.len() - 1 {
            let low = bounds[index];
            let high = bounds[index + 1];
            assert2::assert!(shard_bound_key(low) < shard_bound_key(high));
            assert2::assert!(parse_shard_bound_key(&shard_bound_key(low)) == Some(low));
        }

        for malformed in [
            "",
            "-1",
            "abc",
            "0000000000000000000",
            "000000000000000000000",
        ] {
            assert2::assert!(parse_shard_bound_key(malformed).is_none());
        }
    }

    #[test]
    fn candidate_blocks_answers_what_a_walk_over_every_block_would() {
        // The inverted posting map and the time window are two independent
        // shortcuts over the same question, and either could be wrong in a way
        // that only shows on a shape a hand-written case does not reach:
        // blocks that overlap each other, blocks out of order, fingerprints in
        // some blocks and not others. The oracle is the walk the shortcuts
        // replaced.
        let mut index = Index::new();
        let mut fingerprints = Vec::new();
        for series in 0..64_u64 {
            let labels = Labels::from_pairs([LabelPair::new("pod", format!("pod-{series}"))]);
            fingerprints.push(labels.fingerprint());
            index.add_series("t", labels.fingerprint(), &labels);
        }

        let mut noise = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            noise ^= noise << 13;
            noise ^= noise >> 7;
            noise ^= noise << 17;
            noise
        };
        let mut blocks = Vec::new();
        for block in 0..40_u64 {
            let min_ts = i64::try_from(next() % 500).unwrap();
            let span = i64::try_from(next() % 80).unwrap();
            let meta = BlockMeta {
                tenant: "t".into(),
                object_key: format!("b{block}.parquet"),
                min_ts,
                max_ts: min_ts + span,
                row_count: 1,
                fingerprints: fingerprints
                    .iter()
                    .copied()
                    .filter(|_| next() % 3 == 0)
                    .collect(),
                level: BlockLevel::INGESTED,
            };
            index.add_block(&meta);
            blocks.push(meta);
        }

        for _ in 0..200 {
            let min_ts = i64::try_from(next() % 600).unwrap();
            let max_ts = min_ts + i64::try_from(next() % 200).unwrap();
            let selected = fingerprints
                .iter()
                .copied()
                .filter(|_| next() % 8 == 0)
                .collect::<BTreeSet<_>>();

            let mut want = blocks
                .iter()
                .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
                .filter(|block| block.fingerprints.iter().any(|fp| selected.contains(fp)))
                .map(|block| block.object_key.clone())
                .collect::<Vec<_>>();
            want.sort();

            let mut got = index.candidate_blocks("t", &selected, min_ts, max_ts);
            got.sort();
            assert2::assert!(got == want);
        }
    }

    #[test]
    fn restating_a_block_with_a_different_series_set_replaces_its_postings() {
        // The postings are the only copy of the block-to-series pairs, so a
        // restatement that shrinks a block's set has to drop the pairs it no
        // longer claims. A digest that matched too eagerly would leave the old
        // ones behind and offer the block for a series it no longer holds.
        let mut index = seed();
        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let web_prod =
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]).fingerprint();

        index.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 2,
            fingerprints: vec![api_prod, web_prod],
            level: BlockLevel::INGESTED,
        });
        index.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![web_prod],
            level: BlockLevel::INGESTED,
        });

        assert2::assert!(index.block_count("t") == 1);
        assert2::assert!(
            index
                .candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 100)
                .is_empty()
        );
        assert2::assert!(
            index.candidate_blocks("t", &BTreeSet::from([web_prod]), 0, 100)
                == vec!["b1.parquet".to_string()]
        );
        assert2::assert!(index.all_blocks("t")[0].fingerprints == vec![web_prod]);
    }

    #[tokio::test]
    async fn a_corrupt_shard_is_reported_against_its_own_object() {
        use object_store::{PutPayload, path::Path};

        let saved = saved_index().await;
        let store = &saved.store;
        let key = &saved.key;
        let shard = one_shard_object(store).await;

        // A shard truncated mid-body: the magic and version are right, so this
        // reaches the body decoder rather than the header check.
        let good = store.get(&shard).await.unwrap().bytes().await.unwrap();
        store
            .put(
                &Path::from(shard.as_ref()),
                PutPayload::from(good[..good.len() / 2].to_vec()),
            )
            .await
            .unwrap();

        let Err(BlockStoreError::InvalidBlock(msg)) = Index::load(store, key).await else {
            panic!("expected a truncated shard to be reported");
        };
        assert2::assert!(msg.starts_with(&format!("index shard `{shard}`: ")));
    }

    /// An index of two blocks a shard width apart, saved at a width of 100.
    async fn saved_index() -> SavedIndex {
        use object_store::memory::InMemory;

        let api_prod =
            labels(&[LabelPair::new("app", "api"), LabelPair::new("env", "prod")]).fingerprint();
        let web_prod =
            labels(&[LabelPair::new("app", "web"), LabelPair::new("env", "prod")]).fingerprint();
        let mut index = seed();
        index.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 50,
            row_count: 1,
            fingerprints: vec![api_prod],
            level: BlockLevel::INGESTED,
        });
        index.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 1_000,
            max_ts: 1_050,
            row_count: 1,
            fingerprints: vec![web_prod],
            level: BlockLevel::INGESTED,
        });

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = "index/metrics.json".to_string();
        index
            .save_with_shard_width(&store, &key, 100)
            .await
            .unwrap();
        SavedIndex { store, key }
    }

    async fn shard_object_keys(store: &Arc<dyn ObjectStore>) -> Vec<Path> {
        use futures::StreamExt as _;

        let mut keys = store
            .list(None)
            .map(|meta| meta.unwrap().location)
            .filter(|location| {
                let is_shard = location.as_ref().ends_with("shard.kbi");
                async move { is_shard }
            })
            .collect::<Vec<_>>()
            .await;
        keys.sort();
        keys
    }

    async fn one_shard_object(store: &Arc<dyn ObjectStore>) -> Path {
        shard_object_keys(store)
            .await
            .into_iter()
            .next()
            .expect("a saved index has at least one shard")
    }

    /// A tenant reaches an index key from an untrusted header, and the key is
    /// what separates one tenant's shards from another's.
    #[test]
    fn a_tenant_never_widens_an_index_key_past_its_own_segment() {
        const KEY: &str = "index/metrics.json";
        let shards_prefix = index_shards_prefix_for_key(KEY);
        let range = IndexShardRange::new(10, 20);

        for case in AWKWARD_TENANTS {
            let name = case.name;
            let prefix = index_shard_tenant_prefix(KEY, case.tenant);
            let shard = index_shard_object_key(KEY, case.tenant, range);
            let unbound = index_unbound_series_object_key(KEY, case.tenant);

            for key in [&prefix, &shard, &unbound] {
                let rest = key
                    .strip_prefix(&shards_prefix)
                    .expect("every index key sits under the shard prefix of its key");
                let segments = rest.trim_start_matches('/').split('/').collect::<Vec<_>>();
                assert2::check!(segments[0].starts_with("tenant="), "{name}");
                assert2::check!(
                    !segments
                        .iter()
                        .any(|segment| matches!(*segment, "." | ".." | "")),
                    "{name}"
                );
                // `object_store` stores the key byte for byte, so the key a
                // writer builds is the location a listing hands back.
                assert2::check!(Path::from(key.clone()).as_ref() == key.as_str(), "{name}");
            }
            assert2::check!(
                prefix.matches('/').count() == shards_prefix.matches('/').count() + 1,
                "{name}"
            );
            assert2::check!(
                shard.matches('/').count() == shards_prefix.matches('/').count() + 3,
                "{name}"
            );

            assert2::check!(
                parse_index_shard_location(&shards_prefix, &shard)
                    == Some(parse_index_shard_location::ParsedIndexShardLocation {
                        tenant: case.tenant.to_string(),
                        object: IndexShardObject::Shard(range),
                    }),
                "{name}"
            );
            assert2::check!(
                parse_index_shard_location(&shards_prefix, &unbound)
                    == Some(parse_index_shard_location::ParsedIndexShardLocation {
                        tenant: case.tenant.to_string(),
                        object: IndexShardObject::UnboundSeries,
                    }),
                "{name}"
            );
        }
    }

    #[test]
    fn a_tenant_segment_the_writer_could_not_have_written_is_foreign() {
        const KEY: &str = "index/metrics.json";
        let shards_prefix = index_shards_prefix_for_key(KEY);

        assert2::check!(
            parse_index_shard_location(&shards_prefix, "index/metrics/shards/t/unbound.kbi")
                == None
        );
        assert2::check!(
            parse_index_shard_location(
                &shards_prefix,
                "index/metrics/shards/tenant=a!zz/unbound.kbi"
            ) == None
        );
    }
}

mod anchored_regex;
mod block_entry;
mod block_list;
mod block_list_repr;
mod byte_reader;
mod decode_index_shard;
mod default_index_shard_width;
mod encode_index_shard;
mod fingerprint_set_digest;
mod index_shard_format;
mod index_shard_object;
mod index_shard_object_key;
mod index_shard_payload;
mod index_shard_range;
mod index_shard_tenant_prefix;
mod index_shard_width_for_span;
mod index_shards_prefix_for_key;
mod index_type;
mod index_unbound_series_object_key;
mod load_index_shards;
mod matcher_matches_empty;
mod max_index_shards_per_tenant;
mod max_index_snapshot_bytes;
mod parse_index_shard_location;
mod parse_shard_bound_key;
mod plan_tenant_index_shards;
mod push_dictionary_id;
mod push_ivarint;
mod push_len;
mod push_shard_block_prefix;
mod push_string;
mod push_uvarint;
mod read_index_shard;
mod resolve_selective;
mod save_index_shards;
mod shard_bound_key;
mod tenant_index;

use anchored_regex::anchored_regex;
use block_entry::BlockEntry;
use block_list::BlockList;
use block_list_repr::BlockListRepr;
pub(crate) use byte_reader::{
    ByteReader, ShardBlockBounds, decode_shard_header, read_shard_block_prefix,
};
pub(crate) use decode_index_shard::{decode_index_shard, dictionary_entry};
pub use default_index_shard_width::DEFAULT_INDEX_SHARD_WIDTH;
pub(crate) use encode_index_shard::encode_index_shard;
use fingerprint_set_digest::fingerprint_set_digest;
use index_shard_format::{INDEX_SHARD_FORMAT_VERSION, INDEX_SHARD_MAGIC};
pub(crate) use index_shard_object::IndexShardObject;
pub use index_shard_object_key::index_shard_object_key;
pub(crate) use index_shard_payload::IndexShardPayload;
pub use index_shard_range::IndexShardRange;
pub use index_shard_tenant_prefix::index_shard_tenant_prefix;
use index_shard_width_for_span::index_shard_width_for_span;
pub use index_shards_prefix_for_key::index_shards_prefix_for_key;
pub use index_type::Index;
pub use index_unbound_series_object_key::index_unbound_series_object_key;
use load_index_shards::load_index_shards;
use matcher_matches_empty::matcher_matches_empty;
pub use max_index_shards_per_tenant::MAX_INDEX_SHARDS_PER_TENANT;
pub use max_index_snapshot_bytes::MAX_INDEX_SNAPSHOT_BYTES;
pub(crate) use parse_index_shard_location::parse_index_shard_location;
pub(crate) use parse_shard_bound_key::parse_shard_bound_key;
use plan_tenant_index_shards::plan_tenant_index_shards;
pub(crate) use push_dictionary_id::push_dictionary_id;
pub(crate) use push_ivarint::push_ivarint;
pub(crate) use push_len::push_len;
pub(crate) use push_shard_block_prefix::push_shard_block_prefix;
pub(crate) use push_string::push_string;
pub(crate) use push_uvarint::push_uvarint;
use read_index_shard::read_index_shard;
pub(crate) use read_index_shard::{CappedObject, capped_read_error, oversized_object_error};
pub(crate) use shard_bound_key::shard_bound_key;
use tenant_index::TenantIndex;
