//! Test in-memory profile store.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{LabelMatcher, Labels, MatchOp};
use regex::Regex;

use crate::{
    error::ProfileError,
    store::{ProfileScan, ProfileStats, ProfileStore},
    symbol_db::SymbolDb,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileIdentity<'a> {
    pub tenant: &'a str,
    pub profile_type: &'a str,
}

impl<'a> ProfileIdentity<'a> {
    #[must_use]
    pub const fn new(tenant: &'a str, profile_type: &'a str) -> Self {
        Self {
            tenant,
            profile_type,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StacktraceIdentity {
    pub partition: u64,
    pub stacktrace_id: u32,
}

impl StacktraceIdentity {
    #[must_use]
    pub const fn new(partition: u64, stacktrace_id: u32) -> Self {
        Self {
            partition,
            stacktrace_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleValues {
    pub value: i64,
    pub total_value: i64,
}

impl SampleValues {
    #[must_use]
    pub const fn new(value: i64, total_value: i64) -> Self {
        Self { value, total_value }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use datafusion::arrow::{
        array::AsArray,
        datatypes::{Int64Type, UInt64Type},
    };
    use krabka_blockstore::{LabelMatcher, MatchOp};

    use super::*;
    use crate::{FunctionRec, LineRec, LocationRec, ProfileLabel};

    struct SampleIdentity {
        tenant: &'static str,
        id: &'static str,
        timestamp_ms: i64,
    }

    fn store_with_two_samples() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let n_main = store.symbols_mut().intern_string("main");
        let f_main = store.symbols_mut().intern_function(FunctionRec {
            name: n_main,
            system_name: n_main,
            filename: 0,
            start_line: 0,
        });
        let n_work = store.symbols_mut().intern_string("work");
        let f_work = store.symbols_mut().intern_function(FunctionRec {
            name: n_work,
            system_name: n_work,
            filename: 0,
            start_line: 0,
        });
        let l_main = store.symbols_mut().intern_location(LocationRec {
            address: 0x10,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: f_main,
                line: 1,
            }],
        });
        let l_work = store.symbols_mut().intern_location(LocationRec {
            address: 0x20,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: f_work,
                line: 2,
            }],
        });
        let st_work = store.symbols_mut().intern_stacktrace(0, &[l_work, l_main]);
        let st_main = store.symbols_mut().intern_stacktrace(0, &[l_main]);
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let labels = vec![ProfileLabel::new("service_name", "checkout")];
        store.push_sample(
            ProfileIdentity::new("t", pt),
            labels.clone(),
            StacktraceIdentity::new(0, st_work),
            10,
            1000,
        );
        store.push_sample(
            ProfileIdentity::new("t", pt),
            labels.clone(),
            StacktraceIdentity::new(0, st_work),
            5,
            1000,
        );
        store.push_sample(
            ProfileIdentity::new("t", pt),
            labels,
            StacktraceIdentity::new(0, st_main),
            3,
            1000,
        );
        store
    }

    #[tokio::test]
    async fn query_series_counts_project_profile_ids_without_losing_matching_or_physical_rows() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let public = vec![
            ProfileLabel::new("service_name", "api"),
            ProfileLabel::new("__profile_type__", pt),
        ];
        for sample in [
            SampleIdentity {
                tenant: "t",
                id: "a",
                timestamp_ms: 1000,
            },
            SampleIdentity {
                tenant: "t",
                id: "b",
                timestamp_ms: 2000,
            },
            SampleIdentity {
                tenant: "u",
                id: "c",
                timestamp_ms: 1000,
            },
        ] {
            let mut labels = public.clone();
            labels.push(ProfileLabel::new("__profile_id__", sample.id));
            store.push_sample(
                ProfileIdentity::new(sample.tenant, pt),
                labels,
                StacktraceIdentity::new(0, 0),
                5,
                sample.timestamp_ms,
            );
        }
        let expected = Labels::from_pairs(public.iter().map(|label| {
            krabka_blockstore::LabelPair::new(label.name.clone(), label.value.clone())
        }))
        .fingerprint();
        let analysis = store.query_stats("t", pt, &[], 1000, 2000).await.unwrap();
        assert!(analysis.fingerprints == BTreeSet::from([expected]));
        assert!(analysis.profile_count == 2);
        assert!(analysis.sample_count == 2);
        assert!(analysis.scopes[0].series_count == 2);
        let matching = [LabelMatcher::new("__profile_id__", MatchOp::Eq, "b")];
        assert!(
            store
                .query_stats("t", pt, &matching, 1000, 2000)
                .await
                .unwrap()
                .fingerprints
                == BTreeSet::from([expected])
        );
        let missing = [LabelMatcher::new("__profile_id__", MatchOp::Eq, "c")];
        assert!(
            store
                .query_stats("t", pt, &missing, 1000, 2000)
                .await
                .unwrap()
                .fingerprints
                .is_empty()
        );
        assert!(
            store
                .query_stats("absent", pt, &[], 1000, 2000)
                .await
                .unwrap()
                .fingerprints
                .is_empty()
        );
    }

    #[tokio::test]
    async fn select_registers_samples_table_and_symbols() {
        let store = store_with_two_samples();
        let scan = store
            .select(
                "t",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                &[],
                0,
                5000,
            )
            .await
            .unwrap();
        let df = scan
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {}", scan.samples_table))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let count = out[0].column(0).as_primitive::<Int64Type>().value(0);
        assert!(count == 3);
        assert!(!scan.symbols.resolve(0, 0).is_empty() || !scan.symbols.resolve(0, 1).is_empty());
    }

    #[tokio::test]
    async fn profile_types_and_label_values() {
        let store = store_with_two_samples();
        let pts = store.profile_types("t", 0, 5000).await.unwrap();
        assert!(pts == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string()]);
        let vals = store
            .label_values("t", "service_name", &[], 0, 5000)
            .await
            .unwrap();
        assert!(vals == vec!["checkout".to_string()]);
        let names = store.label_names("t", &[], 0, 5000).await.unwrap();
        assert!(names == vec!["service_name".to_string()]);
        let series = store
            .series(
                "t",
                &[] as &[LabelMatcher],
                &["service_name".to_string()],
                0,
                5000,
            )
            .await
            .unwrap();
        assert!(series == vec![vec![ProfileLabel::new("service_name", "checkout")]]);

        // Empty `label_names` means "return the full label set" (the Pyroscope
        // `/series` convention), mirroring `krabka_blockstore`'s index. It must
        // NOT collapse to a single empty label set (`[{}]`), which breaks
        // Grafana's Pyroscope label autocomplete. All samples here carry the same
        // single label, so the full sets dedup to one series.
        let unprojected = store
            .series("t", &[] as &[LabelMatcher], &[], 0, 5000)
            .await
            .unwrap();
        assert!(unprojected == vec![vec![ProfileLabel::new("service_name", "checkout")]]);
    }

    #[tokio::test]
    async fn range_filter_requires_rows_inside_both_bounds() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        store.push_sample(
            ProfileIdentity::new("t", pt),
            vec![ProfileLabel::new("service_name", "early")],
            StacktraceIdentity::new(0, 0),
            1,
            1000,
        );
        store.push_sample(
            ProfileIdentity::new("t", pt),
            vec![ProfileLabel::new("service_name", "inside")],
            StacktraceIdentity::new(0, 0),
            1,
            2000,
        );

        let values = store
            .label_values("t", "service_name", &[], 1500, 2500)
            .await
            .unwrap();
        let stats = store.stats("t", 1500, 2500).await.unwrap();

        assert!(values == vec!["inside".to_string()]);
        assert!(
            stats
                == crate::ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(2000),
                    newest_profile_time: Some(2000),
                }
        );
    }

    #[tokio::test]
    async fn select_encodes_distinct_fingerprints_for_distinct_label_sets() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        store.push_sample(
            ProfileIdentity::new("t", pt),
            vec![ProfileLabel::new("service_name", "api")],
            StacktraceIdentity::new(0, 0),
            1,
            1000,
        );
        store.push_sample(
            ProfileIdentity::new("t", pt),
            vec![ProfileLabel::new("service_name", "worker")],
            StacktraceIdentity::new(0, 0),
            1,
            1000,
        );
        let scan = store.select("t", pt, &[], 0, 5000).await.unwrap();
        let df = scan
            .ctx
            .sql(&format!(
                "SELECT {} FROM {} ORDER BY {}",
                crate::samples::COL_FINGERPRINT,
                scan.samples_table,
                crate::samples::COL_FINGERPRINT,
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let fingerprints = out[0].column(0).as_primitive::<UInt64Type>();

        assert!(fingerprints.len() == 2);
        assert!(fingerprints.value(0) != fingerprints.value(1));
    }

    #[tokio::test]
    async fn label_matchers_filter_negative_literal_and_regex_cases() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        for service in ["checkout", "api"] {
            store.push_sample(
                ProfileIdentity::new("t", pt),
                vec![ProfileLabel::new("service_name", service)],
                StacktraceIdentity::new(0, 0),
                1,
                1000,
            );
        }

        let neq = store
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Neq, "checkout")],
                0,
                5000,
            )
            .await
            .unwrap();
        let nre = store
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Nre, "check.*")],
                0,
                5000,
            )
            .await
            .unwrap();

        assert!(neq == vec!["api".to_string()]);
        assert!(nre == vec!["api".to_string()]);
    }

    #[tokio::test]
    async fn series_emits_each_label_set_sorted_by_name() {
        // Push a sample whose labels are in ingest insertion order that is NOT
        // sorted by name (`service_name` before `__profile_type__`). Pyroscope's
        // `/series` emits each set's labels SORTED by name, so both the projected
        // and full-label-set forms must come back with `__profile_type__` first
        // (`_` < `s`). This is the exact ordering the Grafana Profiles Drilldown
        // compares against in the pyroscope_differential test.
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let labels = vec![
            ProfileLabel::new("service_name", "api"),
            ProfileLabel::new("__name__", "process_cpu"),
            ProfileLabel::new("__profile_type__", pt),
        ];
        store.push_sample(
            ProfileIdentity::new("t", pt),
            labels,
            StacktraceIdentity::new(0, 0),
            1,
            1000,
        );

        // Projected onto the drilldown's exact label list (request order is
        // `service_name, __profile_type__`) — the response must still be sorted.
        let projected = store
            .series(
                "t",
                &[] as &[LabelMatcher],
                &["service_name".to_string(), "__profile_type__".to_string()],
                0,
                5000,
            )
            .await
            .unwrap();
        assert!(
            projected
                == vec![vec![
                    ProfileLabel::new("__profile_type__", pt),
                    ProfileLabel::new("service_name", "api"),
                ]]
        );

        // Full label set (`label_names=[]`) — also sorted by name.
        let full = store
            .series("t", &[] as &[LabelMatcher], &[], 0, 5000)
            .await
            .unwrap();
        assert!(
            full == vec![vec![
                ProfileLabel::new("__name__", "process_cpu"),
                ProfileLabel::new("__profile_type__", pt),
                ProfileLabel::new("service_name", "api"),
            ]]
        );
    }
}

mod compile_matchers;
mod compiled_matcher;
mod encode_rows;
mod fingerprint_labels;
mod in_memory_profile_store;
mod label_value;
mod row_matches;
mod sample_row;

use compile_matchers::compile_matchers;
use compiled_matcher::CompiledMatcher;
use encode_rows::encode_rows;
use fingerprint_labels::{fingerprint_labels, queried_series_fingerprint};
pub use in_memory_profile_store::{InMemoryProfileStore, SampleAssociations, SampleProvenance};
use label_value::label_value;
use row_matches::row_matches;
use sample_row::SampleRow;
