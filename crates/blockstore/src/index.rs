//! In-memory label/series/block index.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    error::{BlockStoreError, Result},
    labels::{Labels, SeriesFingerprint},
    matcher::{LabelMatcher, MatchOp, QUERY_SHARD_LABEL, parse_query_shard_selector},
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        block::BlockMeta,
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    fn seed() -> Index {
        let mut idx = Index::new();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]);
        let api_dev = labels(&[("app", "api"), ("env", "dev")]);
        let web_prod = labels(&[("app", "web"), ("env", "prod")]);
        idx.add_series("t", api_prod.fingerprint(), &api_prod);
        idx.add_series("t", api_dev.fingerprint(), &api_dev);
        idx.add_series("t", web_prod.fingerprint(), &web_prod);
        idx
    }

    #[test]
    fn snapshot_size_cap_is_256_mib() {
        assert2::assert!(MAX_INDEX_SNAPSHOT_BYTES == mebibytes(256));
        assert2::assert!(MAX_INDEX_SNAPSHOT_BYTES.bytes_u64() == 256 * 1024 * 1024);
    }

    #[test]
    fn resolve_matcher_cases() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        for (_name, tenant, matchers, expected) in [
            (
                "equal intersection",
                "t",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Eq, "prod"),
                ],
                BTreeSet::from([api_prod]),
            ),
            (
                "not equal exclusion",
                "t",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "prod"),
                ],
                BTreeSet::from([api_dev]),
            ),
            (
                "regex union",
                "t",
                vec![LabelMatcher::new("env", MatchOp::Re, "pro.*")],
                BTreeSet::from([api_prod, web_prod]),
            ),
            (
                "unknown tenant",
                "nope",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                BTreeSet::new(),
            ),
        ] {
            assert2::assert!(idx.resolve(tenant, &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn eq_does_not_collide_across_nul_boundary() {
        // `("x", "a\0b")` and `("x\0a", "b")` share the same naive
        // `name\0value` byte string, so an in-band NUL delimiter would index
        // both under one bucket and contaminate Eq results across series.
        let mut idx = Index::new();
        let s1 = labels(&[("x", "a\u{0}b")]);
        let s2 = labels(&[("x\u{0}a", "b")]);
        idx.add_series("t", s1.fingerprint(), &s1);
        idx.add_series("t", s2.fingerprint(), &s2);

        for (_name, matcher, expected) in [
            (
                "NUL in label value",
                LabelMatcher::new("x", MatchOp::Eq, "a\u{0}b"),
                BTreeSet::from([s1.fingerprint()]),
            ),
            (
                "NUL in label name",
                LabelMatcher::new("x\u{0}a", MatchOp::Eq, "b"),
                BTreeSet::from([s2.fingerprint()]),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &[matcher]).unwrap() == expected);
        }
    }

    #[test]
    fn candidate_blocks_prune_by_fp_and_time() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 300,
            row_count: 1,
            fingerprints: vec![web_prod],
        });

        for (_name, min_ts, max_ts, expected) in [
            (
                "matching fingerprint and time",
                0,
                150,
                vec!["b1.parquet".to_string()],
            ),
            ("outside time range", 500, 600, Vec::new()),
        ] {
            assert2::assert!(
                idx.candidate_blocks("t", &BTreeSet::from([api_prod]), min_ts, max_ts) == expected
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
            (
                "not-equal matcher accepts empty",
                vec![LabelMatcher::new("foo", MatchOp::Neq, "bar")],
                false,
            ),
            (
                "equal-empty matcher",
                vec![LabelMatcher::new("foo", MatchOp::Eq, "")],
                false,
            ),
            (
                "match-all regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".*")],
                false,
            ),
            (
                "negative regex accepts empty",
                vec![LabelMatcher::new("foo", MatchOp::Nre, "bar")],
                false,
            ),
            (
                "synthetic shard only",
                vec![LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2")],
                false,
            ),
            (
                "restricting regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".*bar.*")],
                true,
            ),
            (
                "non-empty regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".+")],
                true,
            ),
            (
                "empty matcher paired with restricting matcher",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "dev"),
                ],
                true,
            ),
        ];
        for (_name, matchers, expected_ok) in cases {
            assert2::assert!(idx.resolve("t", &matchers).is_ok() == expected_ok);
        }
    }

    #[test]
    fn tenant_isolation_for_same_labels() {
        let mut idx = Index::new();
        let tenant_a = labels(&[("app", "api"), ("env", "prod")]);
        let tenant_b = labels(&[("app", "api"), ("env", "prod")]);
        let other = labels(&[("app", "web"), ("env", "prod")]);
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
        let original = labels(&[("app", "api")]);
        let replacement = labels(&[("app", "web"), ("env", "prod")]);
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
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let all = BTreeSet::from([api_prod, api_dev, web_prod]);

        // The empty-string matchers below all match the absent label, so each is
        // anchored with a non-empty `app=~".+"` matcher (which selects every
        // seeded series) to form a valid Prometheus vector selector; the anchor
        // does not change the empty-string posting result under test.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for (_name, matcher, expected) in [
            (
                "equal empty",
                LabelMatcher::new("missing", MatchOp::Eq, ""),
                all.clone(),
            ),
            (
                "regex empty",
                LabelMatcher::new("missing", MatchOp::Re, ".*"),
                all,
            ),
            (
                "not equal empty",
                LabelMatcher::new("missing", MatchOp::Neq, ""),
                BTreeSet::new(),
            ),
            (
                "not regex empty",
                LabelMatcher::new("missing", MatchOp::Nre, ".*"),
                BTreeSet::new(),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &[anchor.clone(), matcher]).unwrap() == expected);
        }
    }

    #[test]
    fn present_empty_labels_match_empty_string_semantics() {
        let mut idx = Index::new();
        let empty_zone = labels(&[("app", "api"), ("zone", "")]);
        let absent_zone = labels(&[("app", "web")]);
        let non_empty_zone = labels(&[("app", "db"), ("zone", "us")]);
        idx.add_series("t", empty_zone.fingerprint(), &empty_zone);
        idx.add_series("t", absent_zone.fingerprint(), &absent_zone);
        idx.add_series("t", non_empty_zone.fingerprint(), &non_empty_zone);
        let empty_equivalent =
            BTreeSet::from([empty_zone.fingerprint(), absent_zone.fingerprint()]);

        // `zone=""` matches the empty string, so anchor with a non-empty matcher
        // (`app=~".+"` selects all three series) to form a valid selector.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for (_name, matchers, expected) in [
            (
                "equal empty",
                vec![anchor, LabelMatcher::new("zone", MatchOp::Eq, "")],
                empty_equivalent,
            ),
            (
                "not equal empty",
                vec![LabelMatcher::new("zone", MatchOp::Neq, "")],
                BTreeSet::from([non_empty_zone.fingerprint()]),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn resolve_query_shard_matcher_filters_by_series_fingerprint_modulo() {
        let mut idx = Index::new();
        let series = (0..12)
            .map(|id| labels(&[("app", "api"), ("series", &id.to_string())]))
            .collect::<Vec<_>>();
        for labels in &series {
            idx.add_series("t", labels.fingerprint(), labels);
        }

        let expected = series
            .iter()
            .map(Labels::fingerprint)
            .filter(|fp| fp % 2 == 0)
            .collect::<BTreeSet<_>>();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
                ],
            )
            .unwrap();

        assert2::assert!(!expected.is_empty());
        assert2::assert!(expected.len() < series.len());
        assert2::assert!(got == expected);
    }

    #[test]
    fn resolve_query_shard_not_equal_returns_complement() {
        let mut idx = Index::new();
        let series = (0..12)
            .map(|id| labels(&[("app", "api"), ("series", &id.to_string())]))
            .collect::<Vec<_>>();
        for labels in &series {
            idx.add_series("t", labels.fingerprint(), labels);
        }

        let expected = series
            .iter()
            .map(Labels::fingerprint)
            .filter(|fp| fp % 2 != 0)
            .collect::<BTreeSet<_>>();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
                ],
            )
            .unwrap();

        assert2::assert!(!expected.is_empty());
        assert2::assert!(expected.len() < series.len());
        assert2::assert!(got == expected);
    }

    #[test]
    fn matching_fingerprints_returns_matched_set() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        for (_name, tenant, matchers, expected) in [
            (
                "specific matcher",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                BTreeSet::from([api_prod, api_dev]),
            ),
            (
                "all tenant series",
                "t",
                Vec::new(),
                BTreeSet::from([api_prod, api_dev, web_prod]),
            ),
            ("unknown tenant", "nope", Vec::new(), BTreeSet::new()),
        ] {
            assert2::assert!(idx.matching_fingerprints(tenant, &matchers).unwrap() == expected);
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
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
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
        for (_name, tenant, matchers, expected) in [
            (
                "all api environments",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                vec!["dev".to_string(), "prod".to_string()],
            ),
            (
                "only web environment",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "web")],
                vec!["prod".to_string()],
            ),
            ("unknown tenant", "nope", Vec::new(), Vec::new()),
        ] {
            assert2::assert!(idx.label_values_for(tenant, "env", &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn label_values_for_fingerprints_returns_distinct_values() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
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
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "dev".to_string())
                ],
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "prod".to_string())
                ],
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
                labels(&[("app", "api"), ("env", "prod")]),
                labels(&[("app", "api"), ("env", "dev")]),
            ]
        );

        let mut expected_all = vec![
            labels(&[("app", "api"), ("env", "prod")]),
            labels(&[("app", "api"), ("env", "dev")]),
            labels(&[("app", "web"), ("env", "prod")]),
        ];
        expected_all.sort_by_key(Labels::fingerprint);
        assert2::assert!(idx.series("t", &[]).unwrap() == expected_all);
        assert2::assert!(idx.series("nope", &[]).unwrap() == Vec::new());
    }

    #[test]
    fn series_for_fingerprints_projects_label_values() {
        let idx = seed();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["app".to_string(), "env".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                ("app".to_string(), "web".to_string()),
                ("env".to_string(), "prod".to_string()),
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
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["env".to_string(), "app".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                ("app".to_string(), "web".to_string()),
                ("env".to_string(), "prod".to_string()),
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
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let mut got = idx.series_for_fingerprints("t", &BTreeSet::from([api_prod, api_dev]), &[]);
        got.sort();
        assert2::assert!(
            got == vec![
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "dev".to_string()),
                ],
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn candidate_blocks_for_series_returns_pruned_keys() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
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
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 350,
            row_count: 1,
            fingerprints: vec![],
        });

        for (_name, tenant, min_ts, max_ts, want) in [
            // Window covering both → combined min/max across them.
            ("both blocks", "t", 0, 1_000, Some((10, 350))),
            // Window covering only b1 → exactly b1's bounds (kills Some((x,y)) stubs).
            ("first block", "t", 0, 150, Some((10, 100))),
            // Window that overlaps nothing → None.
            ("no overlap", "t", 500, 600, None),
            // Unknown tenant → None.
            ("unknown tenant", "nope", 0, 1_000, None),
        ] {
            assert2::assert!(idx.block_time_bounds(tenant, min_ts, max_ts) == want);
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
        });

        for (_name, min_ts, max_ts, want) in [
            // Touch the block's max at the window's min: b.min_ts(100) <= max_ts(200)
            // && b.max_ts(200) >= min_ts(200). `<=`→`>` or `>=`→`<` would drop it.
            ("touches maximum", 200, 300, Some((100, 200))),
            // Touch the block's min at the window's max.
            ("touches minimum", 0, 100, Some((100, 200))),
            // A window entirely above the block: with `&&`→`||` this would wrongly
            // include the block (one side still true), so demand None here.
            ("entirely above", 300, 400, None),
            // A window entirely below the block: the other side is the true one.
            ("entirely below", 0, 50, None),
        ] {
            assert2::assert!(idx.block_time_bounds("t", min_ts, max_ts) == want);
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
        });
        idx.add_block(&BlockMeta {
            tenant: "u".into(),
            object_key: "b2.parquet".into(),
            min_ts: 5,
            max_ts: 9,
            row_count: 3,
            fingerprints: vec![],
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
                    },
                    BlockMeta {
                        tenant: "u".to_string(),
                        object_key: "b2.parquet".to_string(),
                        min_ts: 5,
                        max_ts: 9,
                        row_count: 3,
                        fingerprints: vec![],
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
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
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
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let meta = BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        };

        idx.add_block(&meta);
        idx.add_block(&meta);

        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 100);
        assert2::assert!(got == vec!["b1.parquet".to_string()]);
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory};

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        let loaded = Index::load(&store, "index/snapshot.json").await.unwrap();
        let got = loaded
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();

        assert2::assert!(
            got == BTreeSet::from([
                labels(&[("app", "api"), ("env", "prod")]).fingerprint(),
                labels(&[("app", "api"), ("env", "dev")]).fingerprint(),
            ])
        );
    }

    #[tokio::test]
    async fn load_rejects_over_cap_snapshot() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory, path::Path};

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap object; the real snapshot is well above 1 byte.
        let size = store
            .head(&Path::from("index/snapshot.json"))
            .await
            .unwrap()
            .size;
        assert2::assert!(size > 1);

        let got = Index::load_with_cap(&store, "index/snapshot.json", bytes(1)).await;
        let Err(BlockStoreError::InvalidBlock(msg)) = got else {
            panic!("expected InvalidBlock for oversized index snapshot");
        };
        assert2::assert!(
            msg == format!(
                "index snapshot `index/snapshot.json` is {size} bytes, exceeds cap of 1 bytes"
            )
        );

        // A cap at/above the real size still loads.
        let loaded =
            Index::load_with_cap(&store, "index/snapshot.json", ByteSize::from_bytes(size))
                .await
                .unwrap();
        assert2::assert!(loaded.block_count("t") == idx.block_count("t"));
    }

    #[tokio::test]
    async fn load_missing_snapshot_preserves_object_store_error_text() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory, path::Path};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("index/missing.json");
        let expected = store.head(&path).await.unwrap_err().to_string();

        let got =
            Index::load_with_cap(&store, "index/missing.json", MAX_INDEX_SNAPSHOT_BYTES).await;

        let Err(BlockStoreError::ObjectStore(msg)) = got else {
            panic!("expected ObjectStore error for missing index snapshot");
        };
        assert2::assert!(msg == expected);
    }
}

// === split-modules: generated submodules ===
mod anchored_regex;
mod block_entry;
mod index;
mod matcher_matches_empty;
mod max_index_snapshot_bytes;
mod tenant_index;

use anchored_regex::anchored_regex;
use block_entry::BlockEntry;
pub use index::Index;
use matcher_matches_empty::matcher_matches_empty;
pub use max_index_snapshot_bytes::MAX_INDEX_SNAPSHOT_BYTES;
use tenant_index::TenantIndex;
