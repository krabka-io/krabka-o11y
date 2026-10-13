//! Flamegraph merge engine.

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

use arrow::{
    array::AsArray,
    datatypes::{Int64Type, UInt64Type},
};
use krabka_blockstore::{LabelMatcher, MatchOp};
use krabka_units::Time;

use crate::{
    FlameGraph, FlameGraphDiff, Frame, Heatmap, LabeledHeatmap, PprofProfile, ProfileError,
    ProfileStore, ProfileType, ResolvedLocation, Series, SeriesAgg, StacktraceIdentity, Tree,
    bin_heatmap, diff_trees,
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

#[derive(Debug)]
pub(crate) struct SeriesBucket {
    pub(crate) timestamp_ms: i64,
    pub(crate) values: Vec<i64>,
}

pub(crate) fn into_series_buckets(mut buckets: BTreeMap<i64, Vec<i64>>) -> Vec<SeriesBucket> {
    let timestamps = buckets.keys().copied().collect::<Vec<_>>();
    timestamps
        .into_iter()
        .filter_map(|timestamp_ms| {
            buckets.remove(&timestamp_ms).map(|values| SeriesBucket {
                timestamp_ms,
                values,
            })
        })
        .collect()
}

const FRONTEND_RESULT_CACHE_ENTRIES: usize = 256;
const FRONTEND_RESULT_CACHE_TTL: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_units::secs;

    use super::*;

    struct FractionalSample {
        timestamp_ms: i64,
        value: i64,
        service: &'static str,
        instance: &'static str,
    }

    struct StackSample {
        stacktrace_id: u32,
        value: i64,
    }

    struct StackIdentityPair {
        first: u32,
        second: u32,
    }

    struct AssociatedSample {
        stacktrace_id: u32,
        value: i64,
        span_id: u64,
        trace_id: Vec<u8>,
    }

    struct SeriesExpectation {
        agg: SeriesAgg,
        call_sites: Vec<String>,
        api_points: Vec<crate::SamplePoint>,
        lookback_value: f64,
    }

    struct StackSelectorExpectation {
        names: Vec<&'static str>,
        expected_total: i64,
    }

    struct SeriesSelectorExpectation {
        selector: &'static str,
        call_sites: Vec<String>,
    }
    use crate::{
        FunctionRec, InMemoryProfileStore, LineRec, LocationRec, MappingRec, MappingSymbolization,
        MappingSymbolizationParts, SeriesAgg,
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn merge_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stacks = {
            let db = store.symbols_mut();
            let main = intern_location(db, "main");
            let work = intern_location(db, "work");
            let other = intern_location(db, "other");
            StackIdentityPair {
                first: db.intern_stacktrace(0, &[work, main]),
                second: db.intern_stacktrace(0, &[other, main]),
            }
        };
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stacks.first),
            10,
            100,
        );
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stacks.first),
            5,
            110,
        );
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "worker")],
            crate::StacktraceIdentity::new(0, stacks.second),
            3,
            120,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    fn branchy_fixture(default_max_nodes: i64) -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stacks = {
            let db = store.symbols_mut();
            let main = intern_location(db, "main");
            let work = intern_location(db, "work");
            let cold = intern_location(db, "cold_leaf");
            StackIdentityPair {
                first: db.intern_stacktrace(0, &[work, main]),
                second: db.intern_stacktrace(0, &[cold, main]),
            }
        };
        let labels = vec![crate::ProfileLabel::new("service", "api")];
        store.push_sample_with_total_and_span(
            crate::ProfileIdentity::new("tenant-a", PT),
            labels.clone(),
            crate::StacktraceIdentity::new(0, stacks.first),
            crate::SampleValues::new(10, 10),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            crate::ProfileIdentity::new("tenant-a", PT),
            labels.clone(),
            crate::StacktraceIdentity::new(0, stacks.second),
            crate::SampleValues::new(5, 5),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            crate::ProfileIdentity::new("tenant-a", PT),
            labels,
            crate::StacktraceIdentity::new(0, stacks.first),
            crate::SampleValues::new(4, 4),
            30_000,
            111,
        );
        FlameEngine::new(Arc::new(store), EngineOpts { default_max_nodes })
    }

    fn association_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let locations = {
            let db = store.symbols_mut();
            StackIdentityPair {
                first: intern_location(db, "wanted"),
                second: intern_location(db, "other"),
            }
        };
        let wanted = store.symbols_mut().intern_stacktrace(0, &[locations.first]);
        let other = store
            .symbols_mut()
            .intern_stacktrace(0, &[locations.second]);
        let labels = vec![crate::ProfileLabel::new("service", "api")];
        for sample in [
            AssociatedSample {
                stacktrace_id: wanted,
                value: 5,
                span_id: 11,
                trace_id: vec![0xaa; 16],
            },
            AssociatedSample {
                stacktrace_id: other,
                value: 7,
                span_id: 22,
                trace_id: vec![0xbb; 16],
            },
        ] {
            store.push_sample_with_total_and_associations(
                crate::ProfileIdentity::new("tenant-a", PT),
                labels.clone(),
                crate::StacktraceIdentity::new(0, sample.stacktrace_id),
                crate::SampleValues::new(sample.value, sample.value),
                100,
                crate::SampleAssociations::new(Some(sample.span_id), Some(sample.trace_id)),
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

    /// The ids of the one-frame stacks `a` and `b`, both in partition 0.
    struct StacksAB {
        stack_a: u32,
        stack_b: u32,
    }

    struct StoreWithStacks {
        store: InMemoryProfileStore,
        stacks: StacksAB,
    }

    /// An empty store whose symbol database already holds the stacks `a` and
    /// `b`.
    fn store_with_stacks_a_and_b() -> StoreWithStacks {
        let mut store = InMemoryProfileStore::new();
        let db = store.symbols_mut();
        let a = intern_location(db, "a");
        let b = intern_location(db, "b");
        let stacks = StacksAB {
            stack_a: db.intern_stacktrace(0, &[a]),
            stack_b: db.intern_stacktrace(0, &[b]),
        };
        StoreWithStacks { store, stacks }
    }

    #[test]
    fn default_max_nodes_is_2048() {
        assert!(EngineOpts::default().default_max_nodes == 2048);
    }

    #[tokio::test]
    async fn engine_diff_two_windows() {
        let StoreWithStacks {
            mut store,
            stacks: StacksAB { stack_a, stack_b },
        } = store_with_stacks_a_and_b();
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(10, 10),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(10, 15),
            30_000,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, stack_b),
            crate::SampleValues::new(5, 15),
            30_000,
        );
        let engine = FlameEngine::new(
            Arc::new(store),
            EngineOpts {
                default_max_nodes: 2048,
            },
        );

        let diff = engine
            .diff(
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(0, 30_000),
                ),
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(29_000, 60_000),
                ),
                0,
            )
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
        let stacks = {
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
                symbolization: MappingSymbolization::from_parts(MappingSymbolizationParts {
                    functions: true,
                    filenames: true,
                    line_numbers: true,
                    inline_frames: false,
                }),
            });
            let second_mapping = db.intern_mapping(MappingRec {
                memory_start: 0x3000,
                memory_limit: 0x4000,
                file_offset: 0x20,
                filename: second_mapping_file,
                build_id: second_build,
                symbolization: MappingSymbolization::from_parts(MappingSymbolizationParts {
                    functions: true,
                    filenames: true,
                    line_numbers: true,
                    inline_frames: false,
                }),
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
            StackIdentityPair {
                first: db.intern_stacktrace(0, &[first_location]),
                second: db.intern_stacktrace(0, &[second_location]),
            }
        };
        let labels = vec![crate::ProfileLabel::new("service", "api")];
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            labels.clone(),
            crate::StacktraceIdentity::new(0, stacks.first),
            5,
            0,
        );
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            labels,
            crate::StacktraceIdentity::new(0, stacks.second),
            7,
            0,
        );

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
        let StoreWithStacks {
            mut store,
            stacks: StacksAB { stack_a, stack_b },
        } = store_with_stacks_a_and_b();
        store.push_sample_with_total_and_span(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(6, 10),
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, stack_b),
            crate::SampleValues::new(4, 10),
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
            .select_merge_span_profile(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                0,
            )
            .await
            .unwrap();

        assert!(fg.total == 6);
        assert!(
            engine
                .select_merge_span_profile(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    &[],
                    MillisRange::new(0, 60_000),
                    0
                )
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
            .select_merge_span_profile(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                0,
            )
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
                ranges: &[
                    MillisRange::new(0, 10_000),
                    MillisRange::new(10_001, 60_000),
                ],
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
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(2, 5),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, 2),
            crate::SampleValues::new(3, 5),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("svc", "x")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(30, 30),
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
        let stacks = {
            let db = store.symbols_mut();
            let alpha = intern_location(db, "alpha");
            let beta = intern_location(db, "beta");
            StackIdentityPair {
                first: db.intern_stacktrace(0, &[alpha]),
                second: db.intern_stacktrace(1, &[beta]),
            }
        };
        assert!(stacks.first == stacks.second);
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            Vec::new(),
            crate::StacktraceIdentity::new(0, stacks.first),
            5,
            0,
        );
        store.push_sample(
            crate::ProfileIdentity::new("tenant-a", PT),
            Vec::new(),
            crate::StacktraceIdentity::new(1, stacks.second),
            7,
            0,
        );
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
        for expectation in [
            StackSelectorExpectation {
                names: vec!["main", "work"],
                expected_total: 15,
            },
            StackSelectorExpectation {
                names: vec!["main"],
                expected_total: 18,
            },
            StackSelectorExpectation {
                names: vec!["work"],
                expected_total: 0,
            },
            StackSelectorExpectation {
                names: vec!["work", "main"],
                expected_total: 0,
            },
        ] {
            let selectors = expectation
                .names
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>();
            let fg = engine
                .select_merge_stacktraces_with_stack_trace_selector(
                    "tenant-a",
                    PT,
                    "{}",
                    MillisRange::new(0, 200),
                    0,
                    &selectors,
                )
                .await
                .unwrap();
            check!(fg.total == expectation.expected_total);
            if expectation.names == ["main", "work"] {
                check!(self_value_for(&fg, "work") == 15);
                check!(!has_name(&fg, "other"));
            } else if expectation.names == ["main"] {
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
                ProfileSelection::new("tenant-a", PT, "{}"),
                MillisRange::new(0, 200),
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
                ProfileSelection::new("tenant-a", PT, "{}"),
                MillisRange::new(0, 200),
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
            .select_merge_stacktraces_grouped(
                "tenant-a",
                PT,
                "{}",
                MillisRange::new(0, 60_000),
                0,
                &service_group,
            )
            .await
            .unwrap();
        let grouped_limited = engine
            .select_merge_stacktraces_grouped(
                "tenant-a",
                PT,
                "{}",
                MillisRange::new(0, 60_000),
                16,
                &service_group,
            )
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
                MillisRange::new(0, 60_000),
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
                MillisRange::new(0, 60_000),
                16,
                &selector,
            )
            .await
            .unwrap();
        assert!(has_name(&selected_default, "other"));
        assert!(!has_name(&selected_limited, "other"));

        let sharded_default = engine
            .select_merge_stacktraces_sharded(
                "tenant-a",
                PT,
                "{}",
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
                0,
            )
            .await
            .unwrap();
        let sharded_limited = engine
            .select_merge_stacktraces_sharded(
                "tenant-a",
                PT,
                "{}",
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
                16,
            )
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
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 60_000)],
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
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
            .diff(
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(0, 0),
                ),
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(30_000, 30_000),
                ),
                0,
            )
            .await
            .unwrap();
        let diff_limited = engine
            .diff(
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(0, 0),
                ),
                DiffQuery::new(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    MillisRange::new(30_000, 30_000),
                ),
                16,
            )
            .await
            .unwrap();
        assert!(diff_has_name(&diff_default, "other"));
        assert!(!diff_has_name(&diff_limited, "other"));

        let span_default = engine
            .select_merge_span_profile(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                0,
            )
            .await
            .unwrap();
        let span_limited = engine
            .select_merge_span_profile(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                16,
            )
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
                ranges: &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
                ranges: &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
                MillisRange::new(0, 60_000),
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
                MillisRange::new(0, 60_000),
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
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
                ProfileSelection::new("tenant-a", PT, "{}"),
                MillisRange::new(0, 0),
                0,
                &selector,
            )
            .await
            .unwrap();
        let profile_limited = engine
            .select_merge_profile_with_max_nodes_and_stack_trace_selector(
                ProfileSelection::new("tenant-a", PT, "{}"),
                MillisRange::new(0, 0),
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
            .select_merge_span_profile_tree(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                0,
            )
            .await
            .unwrap();
        let span_tree_limited = engine
            .select_merge_span_profile_tree(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &[111],
                MillisRange::new(0, 60_000),
                16,
            )
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
                ranges: &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
                ranges: &[MillisRange::new(0, 0), MillisRange::new(30_000, 30_000)],
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
            .select_merge_stacktraces_sharded(
                "tenant-a",
                PT,
                "{}",
                &[MillisRange::new(0, 105), MillisRange::new(105, 200)],
                2048,
            )
            .await
            .unwrap();

        assert!(sharded == whole);
    }

    #[tokio::test]
    async fn stack_trace_selector_series_sums_selected_stacks_per_profile() {
        let got = branchy_fixture(16)
            .select_series_with_stack_trace_selector(
                ProfileSelection::new("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(15),
                SeriesAgg::Sum,
                MillisRange::new(0, 0),
                &["main".to_string()],
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![crate::SamplePoint {
                    timestamp_ms: 0,
                    value: 15.0
                }],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_average_uses_covering_range_instead_of_summing_shards() {
        let got = branchy_fixture(16)
            .select_series_with_stack_trace_selector_sharded(
                ProfileSelection::new("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(60),
                SeriesAgg::Average,
                &[MillisRange::new(0, 0), MillisRange::new(30_000, 60_000)],
                &["main".to_string()],
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 0,
                        value: 15.0
                    },
                    crate::SamplePoint {
                        timestamp_ms: 60_000,
                        value: 4.0
                    }
                ],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_queries_reject_reversed_ranges() {
        assert!(
            branchy_fixture(16)
                .select_merge_stacktraces_sharded(
                    "tenant-a",
                    PT,
                    "{}",
                    &[MillisRange::new(10, 0)],
                    0
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn grouped_heatmaps_skip_label_sets_without_selected_profile_points() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(5, 5),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", "memory:alloc_space:bytes:space:bytes"),
            vec![crate::ProfileLabel::new("service", "idle")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(9, 9),
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
        check!(got[0].labels == vec![crate::ProfileLabel::new("service", "api")]);
        check!(got[0].heatmap.counts.iter().flatten().sum::<u64>() == 1);
    }

    fn series_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stack_a = 1;
        let stack_b = 2;
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(60, 100),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stack_b),
            crate::SampleValues::new(40, 100),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(50, 50),
            16_000,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "web")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(7, 7),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", "memory:alloc_space:bytes:space:bytes"),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, stack_a),
            crate::SampleValues::new(999, 999),
            0,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    // `series_fixture` summed by `service` at a 15s step.
    fn series_fixture_by_service() -> Vec<Series> {
        vec![
            Series {
                labels: vec![crate::ProfileLabel::new("service", "api")],
                points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 0,
                        value: 100.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 30_000,
                        value: 50.0,
                    },
                ],
            },
            Series {
                labels: vec![crate::ProfileLabel::new("service", "web")],
                points: vec![crate::SamplePoint {
                    timestamp_ms: 0,
                    value: 7.0,
                }],
            },
        ]
    }

    #[tokio::test]
    async fn select_series_sum_buckets_by_step_and_counts_total_once_per_profile() {
        let mut got = series_fixture()
            .select_series(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                MillisRange::new(0, 60_000),
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
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(1, 10),
            0,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(1, 20),
            10_000,
        );
        store.push_sample_with_total(
            crate::ProfileIdentity::new("tenant-a", PT),
            vec![crate::ProfileLabel::new("service", "api")],
            crate::StacktraceIdentity::new(0, 1),
            crate::SampleValues::new(1, 5),
            16_000,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let got = engine
            .select_series(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                MillisRange::new(0, 60_000),
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: vec![crate::ProfileLabel::new("service", "api")],
                points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 0,
                        value: 10.0
                    },
                    crate::SamplePoint {
                        timestamp_ms: 15_000,
                        value: 20.0
                    },
                    crate::SamplePoint {
                        timestamp_ms: 30_000,
                        value: 5.0
                    }
                ],
            }]
        );
    }

    #[tokio::test]
    async fn select_series_average_and_label_selector_bucket_by_step() {
        let got = series_fixture()
            .select_series(
                ProfileSelection::new("tenant-a", PT, r#"{service="api"}"#),
                &[],
                secs(60),
                SeriesAgg::Average,
                MillisRange::new(0, 60_000),
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 0,
                        value: 100.0
                    },
                    crate::SamplePoint {
                        timestamp_ms: 60_000,
                        value: 50.0
                    }
                ],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_select_series_merges_points_for_same_label_set() {
        let mut got = series_fixture()
            .select_series_sharded(
                ProfileSelection::new("tenant-a", PT, "{}"),
                &["service".to_string()],
                secs(15),
                SeriesAgg::Sum,
                &[
                    MillisRange::new(0, 10_000),
                    MillisRange::new(10_000, 60_000),
                ],
            )
            .await
            .unwrap();
        got.sort_by(|left, right| left.labels.cmp(&right.labels));

        assert!(got == series_fixture_by_service());
    }

    fn fractional_series_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stacks = {
            let db = store.symbols_mut();
            let hot = intern_location(db, "hot");
            let cold = intern_location(db, "cold");
            StackIdentityPair {
                first: db.intern_stacktrace(0, &[hot]),
                second: db.intern_stacktrace(0, &[cold]),
            }
        };
        for sample in [
            FractionalSample {
                timestamp_ms: 10,
                value: 256,
                service: "api",
                instance: "a",
            }, // outside the first lookback
            FractionalSample {
                timestamp_ms: 11,
                value: 1,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 1_011,
                value: 2,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 1_012,
                value: 4,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 1_012,
                value: 12,
                service: "api",
                instance: "b",
            }, // unequal profile count at the same time
            FractionalSample {
                timestamp_ms: 2_011,
                value: 8,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 2_012,
                value: 16,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 3_011,
                value: 32,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 3_012,
                value: 64,
                service: "api",
                instance: "a",
            }, // endpoint would be after query end
            FractionalSample {
                timestamp_ms: 3_511,
                value: 128,
                service: "api",
                instance: "a",
            },
            FractionalSample {
                timestamp_ms: 11,
                value: 64,
                service: "lookback-only",
                instance: "a",
            },
        ] {
            let labels = vec![
                crate::ProfileLabel::new("service", sample.service),
                crate::ProfileLabel::new("instance", sample.instance),
            ];
            for stack in [
                StackSample {
                    stacktrace_id: stacks.first,
                    value: sample.value,
                },
                StackSample {
                    stacktrace_id: stacks.second,
                    value: sample.value * 2,
                },
            ] {
                store.push_sample_with_total(
                    crate::ProfileIdentity::new("tenant-a", PT),
                    labels.clone(),
                    crate::StacktraceIdentity::new(0, stack.stacktrace_id),
                    crate::SampleValues::new(stack.value, sample.value * 3),
                    sample.timestamp_ms,
                );
            }
        }
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    #[tokio::test]
    async fn fractional_series_bounds_preserve_profiles_weights_and_global_shard_anchor() {
        let engine = fractional_series_fixture();
        let group_by = ["service".to_string()];
        let shards = [
            MillisRange::new(1_011, 1_600),
            MillisRange::new(1_601, 2_350),
            MillisRange::new(2_351, 3_511),
        ];
        // These independent expectations include a profile exactly at each
        // right endpoint and the one-step lookback, exclude the unfinished
        // final bucket, and weight all three profiles in the middle average.
        for expectation in [
            SeriesExpectation {
                agg: SeriesAgg::Sum,
                call_sites: Vec::new(),
                api_points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 1_011,
                        value: 9.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 2_011,
                        value: 72.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 3_011,
                        value: 144.0,
                    },
                ],
                lookback_value: 192.0,
            },
            SeriesExpectation {
                agg: SeriesAgg::Average,
                call_sites: Vec::new(),
                api_points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 1_011,
                        value: 4.5,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 2_011,
                        value: 24.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 3_011,
                        value: 72.0,
                    },
                ],
                lookback_value: 192.0,
            },
            SeriesExpectation {
                agg: SeriesAgg::Sum,
                call_sites: vec!["hot".to_string()],
                api_points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 1_011,
                        value: 3.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 2_011,
                        value: 24.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 3_011,
                        value: 48.0,
                    },
                ],
                lookback_value: 64.0,
            },
            SeriesExpectation {
                agg: SeriesAgg::Average,
                call_sites: vec!["hot".to_string()],
                api_points: vec![
                    crate::SamplePoint {
                        timestamp_ms: 1_011,
                        value: 1.5,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 2_011,
                        value: 8.0,
                    },
                    crate::SamplePoint {
                        timestamp_ms: 3_011,
                        value: 24.0,
                    },
                ],
                lookback_value: 64.0,
            },
        ] {
            let agg = expectation.agg;
            let call_sites = expectation.call_sites;
            let expected = vec![
                Series {
                    labels: vec![crate::ProfileLabel::new("service", "api")],
                    points: expectation.api_points,
                },
                Series {
                    labels: vec![crate::ProfileLabel::new("service", "lookback-only")],
                    points: vec![crate::SamplePoint {
                        timestamp_ms: 1_011,
                        value: expectation.lookback_value,
                    }],
                },
            ];
            let mut direct = engine
                .select_series_with_stack_trace_selector(
                    ProfileSelection::new("tenant-a", PT, "{}"),
                    &group_by,
                    secs(1),
                    agg,
                    MillisRange::new(1_011, 3_511),
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
                    ProfileSelection::new("tenant-a", PT, "{}"),
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
                ProfileSelection::new("tenant-a", PT, "{}"),
                &group_by,
                secs(1),
                SeriesAgg::Sum,
                &[
                    MillisRange::new(1_012, 1_600),
                    MillisRange::new(1_601, 2_350),
                    MillisRange::new(2_351, 3_511),
                ],
                &["hot".to_string()],
            )
            .await
            .unwrap();
        assert!(
            shifted
                == vec![Series {
                    labels: vec![crate::ProfileLabel::new("service", "api")],
                    points: vec![
                        crate::SamplePoint {
                            timestamp_ms: 1_012,
                            value: 18.0
                        },
                        crate::SamplePoint {
                            timestamp_ms: 2_012,
                            value: 24.0
                        },
                        crate::SamplePoint {
                            timestamp_ms: 3_012,
                            value: 96.0
                        }
                    ],
                }]
        );
        for expectation in [
            SeriesSelectorExpectation {
                selector: r#"{service="missing"}"#,
                call_sites: Vec::new(),
            },
            SeriesSelectorExpectation {
                selector: "{}",
                call_sites: vec!["missing".to_string()],
            },
        ] {
            let empty = engine
                .select_series_with_stack_trace_selector(
                    ProfileSelection::new("tenant-a", PT, expectation.selector),
                    &group_by,
                    secs(1),
                    SeriesAgg::Sum,
                    MillisRange::new(1_011, 3_511),
                    &expectation.call_sites,
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
mod selected_sample_rows;
mod series_buckets_from_stacktrace_selector;
mod series_buckets_from_totals;
mod stack_matches_call_sites;
mod timestamp_total_points;
mod validate_range;

pub use call_site_profile_totals::call_site_profile_totals;
use covering_range::covering_range;
pub use engine_opts::EngineOpts;
pub use flame_engine::{
    DiffQuery, FlameEngine, HeatmapGrid, HeatmapQuery, MillisRange, ProfileSelection,
    SpanProfileShards,
};
use group_frame_name::group_frame_name;
use heatmap_points_from_totals::heatmap_points_from_totals;
use merge_scan_to_pprof::merge_scan_to_pprof;
use merge_scan_to_tree::merge_scan_to_tree;
use merge_sql_to_tree::merge_sql_to_tree;
use sample_selector_sql::sample_selector_sql;
use selected_sample_rows::selected_sample_rows;
use series_buckets_from_stacktrace_selector::series_buckets_from_stacktrace_selector;
use series_buckets_from_totals::series_buckets_from_totals;
pub use stack_matches_call_sites::stack_matches_call_sites;
pub use timestamp_total_points::{TimestampTotalPoint, timestamp_total_points};
use validate_range::validate_range;
