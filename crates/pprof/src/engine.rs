//! Flamegraph merge engine.

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

use arrow::{
    array::{Array, AsArray, BinaryArray},
    datatypes::{Int64Type, UInt64Type},
};
use krabka_blockstore::{LabelMatcher, MatchOp};
use krabka_units::Time;

use crate::{
    FlameGraph, FlameGraphDiff, Frame, Heatmap, LabeledHeatmap, PprofProfile, ProfileError,
    ProfileStore, ProfileType, ResolvedLocation, Series, SeriesAgg, Tree, bin_heatmap, diff_trees,
    raw_profile::resolved_to_pprof_with_max_nodes,
    samples::{
        COL_FINGERPRINT, COL_TIMESTAMP, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
        PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE,
    },
    series::{fold_bucket, step_bucket_ms, validated_step},
};

#[derive(Clone, Copy, Debug, Default)]
pub enum SampleSelector<'a> {
    #[default]
    None,
    Span(&'a [u64]),
    Trace(&'a [Vec<u8>]),
}

/// The samples of one profile scan that a merge keeps.
#[derive(Clone, Copy)]
pub(crate) struct ScanMerge<'a> {
    pub(crate) scan: &'a crate::ProfileScan,
    pub(crate) sample_selector: SampleSelector<'a>,
    /// Keeps only stacks that match these call sites; empty keeps every stack.
    pub(crate) call_sites: &'a [String],
}

const FRONTEND_RESULT_CACHE_ENTRIES: usize = 256;
const FRONTEND_RESULT_CACHE_TTL: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_units::secs;

    use super::*;
    use crate::{
        FunctionRec, InMemoryProfileStore, LineRec, LocationRec, MappingRec, MappingSymbolization,
        SeriesAgg,
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn merge_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (work_stack, other_stack) = {
            let db = store.symbols_mut();
            let main = intern_location(db, "main");
            let work = intern_location(db, "work");
            let other = intern_location(db, "other");
            (
                db.intern_stacktrace(0, &[work, main]),
                db.intern_stacktrace(0, &[other, main]),
            )
        };
        store.push_sample(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, work_stack),
            10,
            100,
        );
        store.push_sample(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, work_stack),
            5,
            110,
        );
        store.push_sample(
            ("tenant-a", PT),
            vec![("service".to_string(), "worker".to_string())],
            (0, other_stack),
            3,
            120,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    fn branchy_fixture(default_max_nodes: i64) -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (work_stack, cold_stack) = {
            let db = store.symbols_mut();
            let main = intern_location(db, "main");
            let work = intern_location(db, "work");
            let cold = intern_location(db, "cold_leaf");
            (
                db.intern_stacktrace(0, &[work, main]),
                db.intern_stacktrace(0, &[cold, main]),
            )
        };
        let labels = vec![("service".to_string(), "api".to_string())];
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            labels.clone(),
            (0, work_stack),
            (10, 10),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            labels.clone(),
            (0, cold_stack),
            (5, 5),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            labels,
            (0, work_stack),
            (4, 4),
            30_000,
            111,
        );
        FlameEngine::new(Arc::new(store), EngineOpts { default_max_nodes })
    }

    fn association_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (wanted, other) = {
            let db = store.symbols_mut();
            (intern_location(db, "wanted"), intern_location(db, "other"))
        };
        let wanted = store.symbols_mut().intern_stacktrace(0, &[wanted]);
        let other = store.symbols_mut().intern_stacktrace(0, &[other]);
        let labels = vec![("service".to_string(), "api".to_string())];
        for (stack, value, span, trace) in [
            (wanted, 5, 11, vec![0xaa; 16]),
            (other, 7, 22, vec![0xbb; 16]),
        ] {
            store.push_sample_with_total_and_associations(
                ("tenant-a", PT),
                labels.clone(),
                (0, stack),
                (value, value),
                100,
                (Some(span), Some(trace)),
            );
        }
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    fn intern_location(db: &mut crate::SymbolDb, name: &str) -> u32 {
        let name_ref = db.intern_string(name);
        let filename_ref = db.intern_string(&format!("{name}.go"));
        let function_id = db.intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: filename_ref,
            start_line: 1,
        });
        db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        })
    }

    fn self_value_for(fg: &FlameGraph, name: &str) -> i64 {
        let name_index = fg
            .names
            .iter()
            .position(|value| value == name)
            .expect("name exists");
        fg.levels
            .iter()
            .flat_map(|level| level.values.as_chunks::<4>().0.iter())
            .find(|chunk| chunk[3] == i64::try_from(name_index).expect("index fits i64"))
            .expect("bar exists")[2]
    }

    fn has_name(fg: &FlameGraph, name: &str) -> bool {
        fg.names.iter().any(|value| value == name)
    }

    fn diff_has_name(diff: &FlameGraphDiff, name: &str) -> bool {
        diff.names.iter().any(|value| value == name)
    }

    fn bytes_contain(bytes: &[u8], needle: &str) -> bool {
        bytes
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
    }

    fn decoded_profile_total(bytes: &[u8]) -> i64 {
        crate::PprofProfile::decode(bytes)
            .unwrap()
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum()
    }

    fn decoded_profile_has_string(bytes: &[u8], value: &str) -> bool {
        crate::PprofProfile::decode(bytes)
            .unwrap()
            .inner()
            .string_table
            .iter()
            .any(|entry| entry == value)
    }

    #[test]
    fn default_max_nodes_is_2048() {
        assert!(EngineOpts::default().default_max_nodes == 2048);
    }

    #[tokio::test]
    async fn engine_diff_two_windows() {
        let mut store = InMemoryProfileStore::new();
        let (stack_a, stack_b) = {
            let db = store.symbols_mut();
            let a = intern_location(db, "a");
            let b = intern_location(db, "b");
            (db.intern_stacktrace(0, &[a]), db.intern_stacktrace(0, &[b]))
        };
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, stack_a),
            (10, 10),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, stack_a),
            (10, 15),
            30_000,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, stack_b),
            (5, 15),
            30_000,
        );
        let engine = FlameEngine::new(
            Arc::new(store),
            EngineOpts {
                default_max_nodes: 2048,
            },
        );

        let diff = engine
            .diff("tenant-a", (PT, "{}", 0, 1), (PT, "{}", 29_000, 60_000), 0)
            .await
            .unwrap();

        check!(diff.left_ticks == 10);
        check!(diff.right_ticks == 15);
        check!(diff.names.iter().any(|name| name == "b"));
    }

    #[tokio::test]
    async fn select_merge_profile_returns_merged_pprof_bytes() {
        let bytes = merge_fixture()
            .select_merge_profile("tenant-a", PT, r#"{service="api"}"#, 0, 200)
            .await
            .unwrap();
        let profile = crate::PprofProfile::decode(&bytes).unwrap();
        let total: i64 = profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();

        assert!(total == 15);
    }

    #[tokio::test]
    async fn select_merge_profile_preserves_distinct_source_locations() {
        let mut store = InMemoryProfileStore::new();
        let (first_stack, second_stack) = {
            let db = store.symbols_mut();
            let function_name = db.intern_string("same_name");
            let first_file = db.intern_string("first.rs");
            let second_file = db.intern_string("second.rs");
            let first_mapping_file = db.intern_string("/bin/first");
            let second_mapping_file = db.intern_string("/bin/second");
            let first_build = db.intern_string("build-first");
            let second_build = db.intern_string("build-second");
            let first_mapping = db.intern_mapping(MappingRec {
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0x10,
                filename: first_mapping_file,
                build_id: first_build,
                symbolization: MappingSymbolization::from_parts((true, true, true, false)),
            });
            let second_mapping = db.intern_mapping(MappingRec {
                memory_start: 0x3000,
                memory_limit: 0x4000,
                file_offset: 0x20,
                filename: second_mapping_file,
                build_id: second_build,
                symbolization: MappingSymbolization::from_parts((true, true, true, false)),
            });
            let first_function = db.intern_function(FunctionRec {
                name: function_name,
                system_name: function_name,
                filename: first_file,
                start_line: 10,
            });
            let second_function = db.intern_function(FunctionRec {
                name: function_name,
                system_name: function_name,
                filename: second_file,
                start_line: 20,
            });
            let first_location = db.intern_location(LocationRec {
                address: 0x1010,
                mapping_id: first_mapping,
                lines: vec![LineRec {
                    function_id: first_function,
                    line: 11,
                }],
            });
            let second_location = db.intern_location(LocationRec {
                address: 0x3020,
                mapping_id: second_mapping,
                lines: vec![LineRec {
                    function_id: second_function,
                    line: 22,
                }],
            });
            (
                db.intern_stacktrace(0, &[first_location]),
                db.intern_stacktrace(0, &[second_location]),
            )
        };
        let labels = vec![("service".to_string(), "api".to_string())];
        store.push_sample(("tenant-a", PT), labels.clone(), (0, first_stack), 5, 0);
        store.push_sample(("tenant-a", PT), labels, (0, second_stack), 7, 0);

        let bytes = FlameEngine::new(Arc::new(store), EngineOpts::default())
            .select_merge_profile("tenant-a", PT, "{}", 0, 1)
            .await
            .unwrap();
        let profile = crate::PprofProfile::decode(&bytes).unwrap();
        let inner = profile.inner();

        check!(inner.sample.len() == 2);
        check!(
            inner
                .location
                .iter()
                .map(|location| location.address)
                .collect::<Vec<_>>()
                == vec![0x1010, 0x3020]
        );
        check!(
            inner
                .location
                .iter()
                .map(|location| location.line[0].line)
                .collect::<Vec<_>>()
                == vec![11, 22]
        );
        check!(inner.mapping.len() == 2);
        check!(inner.string_table.iter().any(|value| value == "first.rs"));
        check!(inner.string_table.iter().any(|value| value == "second.rs"));
        check!(
            inner
                .string_table
                .iter()
                .any(|value| value == "build-first")
        );
        check!(
            inner
                .string_table
                .iter()
                .any(|value| value == "build-second")
        );
    }

    /// Where `span_profile_engine` places its second sample.
    #[derive(Clone, Copy)]
    struct SecondSample {
        timestamp_ms: i64,
        span_id: u64,
    }

    // Two samples of the same series: stack `a` (6 of 10) at 0 ms under span
    // 111, and stack `b` (4 of 10) at `second`.
    fn span_profile_engine(second: SecondSample) -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (stack_a, stack_b) = {
            let db = store.symbols_mut();
            let a = intern_location(db, "a");
            let b = intern_location(db, "b");
            (db.intern_stacktrace(0, &[a]), db.intern_stacktrace(0, &[b]))
        };
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, stack_a),
            (6, 10),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, stack_b),
            (4, 10),
            second.timestamp_ms,
            second.span_id,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    #[tokio::test]
    async fn span_profile_filters_by_span_id() {
        let engine = span_profile_engine(SecondSample {
            timestamp_ms: 0,
            span_id: 222,
        });

        let fg = engine
            .select_merge_span_profile(("tenant-a", PT, "{}"), &[111], (0, 60_000), 0)
            .await
            .unwrap();

        assert!(fg.total == 6);
        assert!(
            engine
                .select_merge_span_profile(("tenant-a", PT, "{}"), &[], (0, 60_000), 0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn sharded_span_profile_matches_whole_range() {
        let engine = span_profile_engine(SecondSample {
            timestamp_ms: 30_000,
            span_id: 111,
        });
        let whole = engine
            .select_merge_span_profile(("tenant-a", PT, "{}"), &[111], (0, 60_000), 0)
            .await
            .unwrap();
        let sharded = engine
            .select_merge_span_profile_sharded(SpanProfileShards {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                span_selector: &[111],
                ranges: &[(0, 10_000), (10_001, 60_000)],
                max_nodes: 0,
            })
            .await
            .unwrap();

        assert!(sharded == whole);
    }

    #[tokio::test]
    async fn select_heatmap_bins_profile_totals() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, 1),
            (2, 5),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, 2),
            (3, 5),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("svc".to_string(), "x".to_string())],
            (0, 1),
            (30, 30),
            60,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let heatmap = engine
            .select_heatmap(HeatmapQuery {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                range: MillisRange {
                    start_ms: 0,
                    end_ms: 100,
                },
                grid: HeatmapGrid {
                    time_buckets: 2,
                    value_buckets: 2,
                },
            })
            .await
            .unwrap();

        assert!(heatmap.counts[0][0] == 1);
        assert!(heatmap.counts[1][1] == 1);
    }

    #[tokio::test]
    async fn raw_ids_never_cross_a_partition_boundary() {
        let mut store = InMemoryProfileStore::new();
        let (alpha_stack, beta_stack) = {
            let db = store.symbols_mut();
            let alpha = intern_location(db, "alpha");
            let beta = intern_location(db, "beta");
            (
                db.intern_stacktrace(0, &[alpha]),
                db.intern_stacktrace(1, &[beta]),
            )
        };
        assert!(alpha_stack == beta_stack);
        store.push_sample(("tenant-a", PT), Vec::new(), (0, alpha_stack), 5, 0);
        store.push_sample(("tenant-a", PT), Vec::new(), (1, beta_stack), 7, 0);
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 60_000, 0)
            .await
            .unwrap();

        check!(fg.names.iter().any(|name| name == "alpha"));
        check!(fg.names.iter().any(|name| name == "beta"));
        check!(fg.total == 12);
    }

    #[tokio::test]
    async fn merge_folds_duplicate_ids_before_symbolize() {
        let fg = merge_fixture()
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 200, 2048)
            .await
            .unwrap();

        check!(fg.total == 18);
        check!(fg.levels[0].values == vec![0, 18, 0, 0]);
        check!(self_value_for(&fg, "work") == 15);
    }

    #[tokio::test]
    async fn merge_applies_label_selector_and_max_nodes_fallback() {
        let fg = merge_fixture()
            .select_merge_stacktraces("tenant-a", PT, r#"{service="api"}"#, 0, 200, 0)
            .await
            .unwrap();

        check!(fg.total == 15);
        check!(fg.names[0] == "total");
        check!(self_value_for(&fg, "work") == 15);
        check!(!fg.names.iter().any(|name| name == "other"));
    }

    #[tokio::test]
    async fn merge_stack_trace_selector_filters_root_first_call_sites() {
        let engine = merge_fixture();
        for (names, expected) in [
            (vec!["main", "work"], 15),
            (vec!["main"], 18),
            (vec!["work"], 0),
            (vec!["work", "main"], 0),
        ] {
            let selectors = names
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>();
            let fg = engine
                .select_merge_stacktraces_with_stack_trace_selector(
                    "tenant-a",
                    PT,
                    "{}",
                    (0, 200),
                    0,
                    &selectors,
                )
                .await
                .unwrap();
            check!(fg.total == expected);
            if names == ["main", "work"] {
                check!(self_value_for(&fg, "work") == 15);
                check!(!has_name(&fg, "other"));
            } else if names == ["main"] {
                check!(self_value_for(&fg, "work") == 15);
                check!(self_value_for(&fg, "other") == 3);
            } else {
                check!(!has_name(&fg, "work"));
                check!(!has_name(&fg, "other"));
            }
        }
    }

    #[tokio::test]
    async fn sample_selectors_filter_flamegraph_and_pprof() {
        let engine = association_fixture();
        let traces = [vec![0xaa; 16]];
        let flamegraph = engine
            .select_merge_stacktraces_with_selectors(
                ("tenant-a", PT, "{}"),
                (0, 200),
                0,
                &[],
                SampleSelector::Trace(&traces),
            )
            .await
            .unwrap();
        check!(flamegraph.total == 5);
        check!(has_name(&flamegraph, "wanted"));
        check!(!has_name(&flamegraph, "other"));

        let profile = engine
            .select_merge_profile_with_selectors(
                ("tenant-a", PT, "{}"),
                (0, 200),
                0,
                &[],
                SampleSelector::Span(&[22]),
            )
            .await
            .unwrap();
        check!(decoded_profile_total(&profile) == 7);
        check!(decoded_profile_has_string(&profile, "other"));
        check!(!decoded_profile_has_string(&profile, "wanted"));
    }

    #[tokio::test]
    async fn max_nodes_zero_uses_default_for_stacktrace_wrappers() {
        let engine = branchy_fixture(3);
        let selector = ["main".to_string()];
        let service_group = ["service".to_string()];

        let grouped_default = engine
            .select_merge_stacktraces_grouped("tenant-a", PT, "{}", (0, 60_000), 0, &service_group)
            .await
            .unwrap();
        let grouped_limited = engine
            .select_merge_stacktraces_grouped("tenant-a", PT, "{}", (0, 60_000), 16, &service_group)
            .await
            .unwrap();
        check!(has_name(&grouped_default, "api"));
        check!(has_name(&grouped_default, "other"));
        check!(has_name(&grouped_limited, "api"));
        check!(!has_name(&grouped_limited, "other"));

        let selected_default = engine
            .select_merge_stacktraces_with_stack_trace_selector(
                "tenant-a",
                PT,
                "{}",
                (0, 60_000),
                0,
                &selector,
            )
            .await
            .unwrap();
        let selected_limited = engine
            .select_merge_stacktraces_with_stack_trace_selector(
                "tenant-a",
                PT,
                "{}",
                (0, 60_000),
                16,
                &selector,
            )
            .await
            .unwrap();
        assert!(has_name(&selected_default, "other"));
        assert!(!has_name(&selected_limited, "other"));

        let sharded_default = engine
            .select_merge_stacktraces_sharded("tenant-a", PT, "{}", &[(0, 0), (30_000, 30_000)], 0)
            .await
            .unwrap();
        let sharded_limited = engine
            .select_merge_stacktraces_sharded("tenant-a", PT, "{}", &[(0, 0), (30_000, 30_000)], 16)
            .await
            .unwrap();
        check!(has_name(&sharded_default, "other"));
        check!(has_name(&sharded_default, "main"));
        check!(!has_name(&sharded_limited, "other"));

        let selected_sharded_default = engine
            .select_merge_stacktraces_with_stack_trace_selector_sharded(
                "tenant-a",
                PT,
                "{}",
                &[(0, 0), (30_000, 60_000)],
                0,
                &selector,
            )
            .await
            .unwrap();
        let selected_sharded_limited = engine
            .select_merge_stacktraces_with_stack_trace_selector_sharded(
                "tenant-a",
                PT,
                "{}",
                &[(0, 0), (30_000, 30_000)],
                16,
                &selector,
            )
            .await
            .unwrap();
        check!(has_name(&selected_sharded_default, "other"));
        check!(has_name(&selected_sharded_default, "main"));
        check!(!has_name(&selected_sharded_limited, "other"));
    }

    #[tokio::test]
    async fn max_nodes_zero_uses_default_for_diff_and_span_wrappers() {
        let engine = branchy_fixture(3);
        let diff_default = engine
            .diff("tenant-a", (PT, "{}", 0, 0), (PT, "{}", 30_000, 30_000), 0)
            .await
            .unwrap();
        let diff_limited = engine
            .diff("tenant-a", (PT, "{}", 0, 0), (PT, "{}", 30_000, 30_000), 16)
            .await
            .unwrap();
        assert!(diff_has_name(&diff_default, "other"));
        assert!(!diff_has_name(&diff_limited, "other"));

        let span_default = engine
            .select_merge_span_profile(("tenant-a", PT, "{}"), &[111], (0, 60_000), 0)
            .await
            .unwrap();
        let span_limited = engine
            .select_merge_span_profile(("tenant-a", PT, "{}"), &[111], (0, 60_000), 16)
            .await
            .unwrap();
        let span_sharded_default = engine
            .select_merge_span_profile_sharded(SpanProfileShards {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                span_selector: &[111],
                ranges: &[(0, 0), (30_000, 30_000)],
                max_nodes: 0,
            })
            .await
            .unwrap();
        let span_sharded_limited = engine
            .select_merge_span_profile_sharded(SpanProfileShards {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                span_selector: &[111],
                ranges: &[(0, 0), (30_000, 30_000)],
                max_nodes: 16,
            })
            .await
            .unwrap();
        check!(has_name(&span_default, "other"));
        check!(!has_name(&span_limited, "other"));
        check!(has_name(&span_sharded_default, "other"));
        check!(!has_name(&span_sharded_limited, "other"));
    }

    #[tokio::test]
    async fn tree_and_pprof_byte_wrappers_apply_selectors_and_max_nodes() {
        let engine = branchy_fixture(3);
        let selector = ["main".to_string()];

        let tree_default = engine
            .select_merge_stacktraces_tree_with_stack_trace_selector(
                "tenant-a",
                PT,
                "{}",
                (0, 60_000),
                0,
                &selector,
            )
            .await
            .unwrap();
        let tree_limited = engine
            .select_merge_stacktraces_tree_with_stack_trace_selector(
                "tenant-a",
                PT,
                "{}",
                (0, 60_000),
                16,
                &selector,
            )
            .await
            .unwrap();
        check!(!bytes_contain(&tree_default, "cold_leaf"));
        check!(bytes_contain(&tree_default, "other"));
        check!(bytes_contain(&tree_default, "main"));
        check!(bytes_contain(&tree_limited, "main"));
        check!(bytes_contain(&tree_limited, "cold_leaf"));
        check!(!bytes_contain(&tree_limited, "other"));

        let sharded_tree_default = engine
            .select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
                "tenant-a",
                PT,
                "{}",
                &[(0, 0), (30_000, 30_000)],
                0,
                &selector,
            )
            .await
            .unwrap();
        let sharded_tree = engine
            .select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
                "tenant-a",
                PT,
                "{}",
                &[(0, 0), (30_000, 30_000)],
                16,
                &selector,
            )
            .await
            .unwrap();
        check!(bytes_contain(&sharded_tree_default, "other"));
        check!(bytes_contain(&sharded_tree_default, "main"));
        check!(bytes_contain(&sharded_tree, "cold_leaf"));
        check!(!bytes_contain(&sharded_tree, "other"));

        let profile_default = engine
            .select_merge_profile_with_max_nodes_and_stack_trace_selector(
                ("tenant-a", PT, "{}"),
                (0, 0),
                0,
                &selector,
            )
            .await
            .unwrap();
        let profile_limited = engine
            .select_merge_profile_with_max_nodes_and_stack_trace_selector(
                ("tenant-a", PT, "{}"),
                (0, 0),
                16,
                &selector,
            )
            .await
            .unwrap();
        check!(decoded_profile_total(&profile_default) == 15);
        check!(decoded_profile_total(&profile_limited) == 15);
        check!(decoded_profile_has_string(&profile_default, "other"));
        check!(decoded_profile_has_string(&profile_default, "main"));
        check!(!decoded_profile_has_string(&profile_limited, "other"));

        let span_tree_default = engine
            .select_merge_span_profile_tree(("tenant-a", PT, "{}"), &[111], (0, 60_000), 0)
            .await
            .unwrap();
        let span_tree_limited = engine
            .select_merge_span_profile_tree(("tenant-a", PT, "{}"), &[111], (0, 60_000), 16)
            .await
            .unwrap();
        check!(bytes_contain(&span_tree_default, "other"));
        check!(bytes_contain(&span_tree_default, "main"));
        check!(!bytes_contain(&span_tree_limited, "other"));

        let span_tree_sharded_default = engine
            .select_merge_span_profile_tree_sharded(SpanProfileShards {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                span_selector: &[111],
                ranges: &[(0, 0), (30_000, 30_000)],
                max_nodes: 0,
            })
            .await
            .unwrap();
        let span_tree_sharded = engine
            .select_merge_span_profile_tree_sharded(SpanProfileShards {
                selection: ProfileSelection {
                    tenant: "tenant-a",
                    profile_type: PT,
                    label_selector: "{}",
                },
                span_selector: &[111],
                ranges: &[(0, 0), (30_000, 30_000)],
                max_nodes: 16,
            })
            .await
            .unwrap();
        check!(bytes_contain(&span_tree_sharded_default, "other"));
        check!(bytes_contain(&span_tree_sharded_default, "main"));
        check!(bytes_contain(&span_tree_sharded, "cold_leaf"));
        check!(!bytes_contain(&span_tree_sharded, "other"));
    }

    #[tokio::test]
    async fn sharded_merge_matches_whole_range_merge() {
        let engine = merge_fixture();
        let whole = engine
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 200, 2048)
            .await
            .unwrap();
        let sharded = engine
            .select_merge_stacktraces_sharded("tenant-a", PT, "{}", &[(0, 105), (105, 200)], 2048)
            .await
            .unwrap();

        assert!(sharded == whole);
    }

    #[tokio::test]
    async fn stack_trace_selector_series_sums_selected_stacks_per_profile() {
        let got = branchy_fixture(16)
            .select_series_with_stack_trace_selector(
                ("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(15),
                SeriesAgg::Sum,
                (0, 0),
                &["main".to_string()],
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![(0, 15.0)],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_average_uses_covering_range_instead_of_summing_shards() {
        let got = branchy_fixture(16)
            .select_series_with_stack_trace_selector_sharded(
                ("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(60),
                SeriesAgg::Average,
                &[(0, 0), (30_000, 60_000)],
                &["main".to_string()],
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![(0, 15.0), (60_000, 4.0)],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_queries_reject_reversed_ranges() {
        assert!(
            branchy_fixture(16)
                .select_merge_stacktraces_sharded("tenant-a", PT, "{}", &[(10, 0)], 0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn grouped_heatmaps_skip_label_sets_without_selected_profile_points() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, 1),
            (5, 5),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", "memory:alloc_space:bytes:space:bytes"),
            vec![("service".to_string(), "idle".to_string())],
            (0, 1),
            (9, 9),
            0,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let got = engine
            .select_heatmaps(
                HeatmapQuery {
                    selection: ProfileSelection {
                        tenant: "tenant-a",
                        profile_type: PT,
                        label_selector: "{}",
                    },
                    range: MillisRange {
                        start_ms: 0,
                        end_ms: 60_000,
                    },
                    grid: HeatmapGrid {
                        time_buckets: 2,
                        value_buckets: 2,
                    },
                },
                &["service".to_string()],
            )
            .await
            .unwrap();

        assert!(got.len() == 1);
        check!(got[0].labels == vec![("service".to_string(), "api".to_string())]);
        check!(got[0].heatmap.counts.iter().flatten().sum::<u64>() == 1);
    }

    fn series_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stack_a = 1;
        let stack_b = 2;
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, stack_a),
            (60, 100),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, stack_b),
            (40, 100),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, stack_a),
            (50, 50),
            16_000,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "web".to_string())],
            (0, stack_a),
            (7, 7),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", "memory:alloc_space:bytes:space:bytes"),
            vec![("service".to_string(), "api".to_string())],
            (0, stack_a),
            (999, 999),
            0,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    // `series_fixture` summed by `service` at a 15s step.
    fn series_fixture_by_service() -> Vec<Series> {
        vec![
            Series {
                labels: vec![("service".to_string(), "api".to_string())],
                points: vec![(0, 100.0), (30_000, 50.0)],
            },
            Series {
                labels: vec![("service".to_string(), "web".to_string())],
                points: vec![(0, 7.0)],
            },
        ]
    }

    #[tokio::test]
    async fn select_series_sum_buckets_by_step_and_counts_total_once_per_profile() {
        let mut got = series_fixture()
            .select_series(
                ("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                (0, 60_000),
            )
            .await
            .unwrap();
        got.sort_by(|left, right| left.labels.cmp(&right.labels));

        assert!(got == series_fixture_by_service());
    }

    #[tokio::test]
    async fn select_series_uses_query_anchored_right_endpoints() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, 1),
            (1, 10),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, 1),
            (1, 20),
            10_000,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service".to_string(), "api".to_string())],
            (0, 1),
            (1, 5),
            16_000,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let got = engine
            .select_series(
                ("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                (0, 60_000),
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: vec![("service".to_string(), "api".to_string())],
                points: vec![(0, 10.0), (15_000, 20.0), (30_000, 5.0)],
            }]
        );
    }

    #[tokio::test]
    async fn select_series_average_and_label_selector_bucket_by_step() {
        let got = series_fixture()
            .select_series(
                ("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(60),
                SeriesAgg::Average,
                (0, 60_000),
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![(0, 100.0), (60_000, 50.0)],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_select_series_merges_points_for_same_label_set() {
        let mut got = series_fixture()
            .select_series_sharded(
                ("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                &[(0, 10_000), (10_000, 60_000)],
            )
            .await
            .unwrap();
        got.sort_by(|left, right| left.labels.cmp(&right.labels));

        assert!(got == series_fixture_by_service());
    }

    fn fractional_series_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (hot, cold) = {
            let db = store.symbols_mut();
            let hot = intern_location(db, "hot");
            let cold = intern_location(db, "cold");
            (
                db.intern_stacktrace(0, &[hot]),
                db.intern_stacktrace(0, &[cold]),
            )
        };
        for (timestamp, value, service, instance) in [
            (10, 256, "api", "a"), // outside the first lookback
            (11, 1, "api", "a"),
            (1_011, 2, "api", "a"),
            (1_012, 4, "api", "a"),
            (1_012, 12, "api", "b"), // unequal profile count at the same time
            (2_011, 8, "api", "a"),
            (2_012, 16, "api", "a"),
            (3_011, 32, "api", "a"),
            (3_012, 64, "api", "a"), // endpoint would be after query end
            (3_511, 128, "api", "a"),
            (11, 64, "lookback-only", "a"),
        ] {
            let labels = vec![
                ("service".to_string(), service.to_string()),
                ("instance".to_string(), instance.to_string()),
            ];
            for (stack, self_value) in [(hot, value), (cold, value * 2)] {
                store.push_sample_with_total(
                    ("tenant-a", PT),
                    labels.clone(),
                    (0, stack),
                    (self_value, value * 3),
                    timestamp,
                );
            }
        }
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    #[tokio::test]
    async fn fractional_series_bounds_preserve_profiles_weights_and_global_shard_anchor() {
        let engine = fractional_series_fixture();
        let group_by = ["service".to_string()];
        let shards = [(1_011, 1_600), (1_601, 2_350), (2_351, 3_511)];
        // These independent expectations include a profile exactly at each
        // right endpoint and the one-step lookback, exclude the unfinished
        // final bucket, and weight all three profiles in the middle average.
        for (agg, call_sites, api_points, lookback) in [
            (
                SeriesAgg::Sum,
                Vec::new(),
                vec![(1_011, 9.0), (2_011, 72.0), (3_011, 144.0)],
                192.0,
            ),
            (
                SeriesAgg::Average,
                Vec::new(),
                vec![(1_011, 4.5), (2_011, 24.0), (3_011, 72.0)],
                192.0,
            ),
            (
                SeriesAgg::Sum,
                vec!["hot".to_string()],
                vec![(1_011, 3.0), (2_011, 24.0), (3_011, 48.0)],
                64.0,
            ),
            (
                SeriesAgg::Average,
                vec!["hot".to_string()],
                vec![(1_011, 1.5), (2_011, 8.0), (3_011, 24.0)],
                64.0,
            ),
        ] {
            let expected = vec![
                Series {
                    labels: vec![("service".to_string(), "api".to_string())],
                    points: api_points,
                },
                Series {
                    labels: vec![("service".to_string(), "lookback-only".to_string())],
                    points: vec![(1_011, lookback)],
                },
            ];
            let mut direct = engine
                .select_series_with_stack_trace_selector(
                    ("tenant-a", PT, "{}"),
                    &group_by,
                    secs(1),
                    agg,
                    (1_011, 3_511),
                    &call_sites,
                )
                .await
                .unwrap();
            direct.sort_by(|left, right| left.labels.cmp(&right.labels));
            assert!(
                direct == expected,
                "direct {agg:?}, {call_sites:?}: {direct:?}"
            );
            let mut sharded = engine
                .select_series_with_stack_trace_selector_sharded(
                    ("tenant-a", PT, "{}"),
                    &group_by,
                    secs(1),
                    agg,
                    &shards,
                    &call_sites,
                )
                .await
                .unwrap();
            sharded.sort_by(|left, right| left.labels.cmp(&right.labels));
            assert!(
                sharded == expected,
                "sharded {agg:?}, {call_sites:?}: {sharded:?}"
            );
        }

        // Reuse two exact shard ranges with a different global start. Cached
        // partial answers must retain the new anchor rather than old endpoints.
        let shifted = engine
            .select_series_with_stack_trace_selector_sharded(
                ("tenant-a", PT, "{}"),
                &group_by,
                secs(1),
                SeriesAgg::Sum,
                &[(1_012, 1_600), (1_601, 2_350), (2_351, 3_511)],
                &["hot".to_string()],
            )
            .await
            .unwrap();
        assert!(
            shifted
                == vec![Series {
                    labels: vec![("service".to_string(), "api".to_string())],
                    points: vec![(1_012, 18.0), (2_012, 24.0), (3_012, 96.0)],
                }]
        );
        for (selector, call_sites) in [
            (r#"{service="missing"}"#, Vec::new()),
            ("{}", vec!["missing".to_string()]),
        ] {
            let empty = engine
                .select_series_with_stack_trace_selector(
                    ("tenant-a", PT, selector),
                    &group_by,
                    secs(1),
                    SeriesAgg::Sum,
                    (1_011, 3_511),
                    &call_sites,
                )
                .await
                .unwrap();
            assert!(empty.is_empty());
        }
    }
}

mod call_site_profile_totals;
mod covering_range;
mod engine_opts;
mod flame_engine;
mod group_frame_name;
mod heatmap_points_from_totals;
mod merge_scan_to_pprof;
mod merge_scan_to_tree;
mod merge_sql_to_tree;
mod sample_selector_sql;
mod series_buckets_from_stacktrace_selector;
mod series_buckets_from_totals;
mod stack_matches_call_sites;
mod timestamp_total_points;
mod validate_range;

pub use call_site_profile_totals::call_site_profile_totals;
use covering_range::covering_range;
pub use engine_opts::EngineOpts;
pub use flame_engine::{
    FlameEngine, HeatmapGrid, HeatmapQuery, MillisRange, ProfileSelection, SpanProfileShards,
};
use group_frame_name::group_frame_name;
use heatmap_points_from_totals::heatmap_points_from_totals;
use merge_scan_to_pprof::merge_scan_to_pprof;
use merge_scan_to_tree::merge_scan_to_tree;
use merge_sql_to_tree::merge_sql_to_tree;
use sample_selector_sql::sample_selector_sql;
use series_buckets_from_stacktrace_selector::series_buckets_from_stacktrace_selector;
use series_buckets_from_totals::series_buckets_from_totals;
pub use stack_matches_call_sites::stack_matches_call_sites;
pub use timestamp_total_points::timestamp_total_points;
use validate_range::validate_range;
