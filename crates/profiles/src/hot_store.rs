//! Live WAL-tail backed profile store.

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use krabka_blockstore::LabelMatcher;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_pprof::{InMemoryProfileStore, ProfileError, ProfileScan, ProfileStats, ProfileStore};
use krabka_units::{Time, convert::TimeExt as _, hours};

use crate::{
    blockbuilder::{intern_record, profile_timestamp_ms},
    error::ProfilesError,
    wal::{PROFILES_WAL_TOPIC, ProfileRecord},
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray};
    use assert2::{assert, check};
    use krabka_blockstore::{LabelMatcher, MatchOp};
    use krabka_pprof::{EngineOpts, FlameEngine, PCOL_TRACE_ID, ProfileStore, SeriesAgg};
    use krabka_units::{Time, convert::TimeExt as _, secs};

    use crate::wal::{ProfileRecord, WalFunction, WalLocation, WalSample, WalSymbolSet};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    /// A retention window so long nothing can age out of it, for the tests that
    /// exercise the record-count budget in isolation. The pruning horizon
    /// saturates to `i64::MIN`, so no timestamp is ever below it.
    fn unlimited_max_age() -> Time {
        Time::from_millis(i64::MAX)
    }

    fn record() -> ProfileRecord {
        ProfileRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("service_name".to_string(), "api".to_string()),
                ("__profile_type__".to_string(), PT.to_string()),
            ],
            profile_type: PT.to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0],
                value: 9,
                timestamp_ns: 1_700_000_000_000,
                span_id: Some(42),
                trace_id: Some(vec![0xaa; 16]),
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "hot_fn".to_string(), "hot.rs".to_string()],
                functions: vec![WalFunction {
                    name: 1,
                    system_name: 1,
                    filename: 2,
                    start_line: 1,
                }],
                locations: vec![WalLocation {
                    address: 0x40,
                    mapping_id: 0,
                    lines: vec![(0, 11)],
                }],
                mappings: vec![],
            },
        }
    }

    /// A rebuild is amortized: it happens once evictions reach a
    /// `1 / REBUILD_AMORTIZE_FACTOR` share of what is still retained, and
    /// never when nothing has been evicted. Both halves of that condition are
    /// pinned at their edge, since a rebuild on every eviction and a rebuild
    /// that never fires both leave queries answering correctly, just slower
    /// or hungrier.
    #[test]
    fn a_rebuild_waits_until_evictions_are_worth_it() {
        let state = |evicted, retained| super::RetainedState {
            records: std::iter::repeat_with(|| super::Retained {
                max_ts_ms: 0,
                record: record(),
            })
            .take(retained)
            .collect(),
            evicted_since_rebuild: evicted,
        };
        let should = |evicted, retained| {
            super::WalTailProfileStore::should_rebuild(&state(evicted, retained))
        };

        // Nothing evicted is never worth a rebuild, however small the store.
        check!(!should(0, 0), "an empty store at rest");
        check!(!should(0, 8), "a full store at rest");

        // One eviction covers eight retained records, so that is the edge.
        check!(!should(1, 9), "one eviction does not cover nine");
        check!(should(1, 8), "one eviction exactly covers eight");
        check!(should(1, 7), "and more than covers seven");
        check!(should(2, 16), "the ratio holds as both grow");
        check!(!should(2, 17), "and so does the edge above it");
    }

    /// The four metadata queries all delegate to the current snapshot,
    /// passing the tenant, matchers and time window straight through. Each is
    /// checked against a store holding two tenants at two different times, so
    /// a delegation that drops an argument, swaps the window ends, or answers
    /// from the wrong tenant returns something visibly different.
    #[tokio::test]
    async fn metadata_queries_respect_tenant_and_time_window() {
        use krabka_pprof::ProfileStore as _;

        let store = super::WalTailProfileStore::new();

        let mut early = record();
        early.samples[0].timestamp_ns = 1_000_000_000;
        store.append_record(early).unwrap();

        let mut late = record();
        late.tenant = "tenant-b".to_string();
        late.labels = vec![
            ("__name__".to_string(), "process_cpu".to_string()),
            ("region".to_string(), "eu".to_string()),
            ("__profile_type__".to_string(), PT.to_string()),
        ];
        late.samples[0].timestamp_ns = 9_000_000_000;
        store.append_record(late).unwrap();

        // Milliseconds, and wide enough to cover both records.
        let (all_start, all_end) = (0_i64, 10_000_i64);

        let names = store
            .label_names("tenant-a", &[], all_start, all_end)
            .await
            .unwrap();
        check!(
            names.contains(&"service_name".to_string()),
            "got: {names:?}"
        );
        check!(
            !names.contains(&"region".to_string()),
            "tenant-b's label must not leak"
        );

        let names = store
            .label_names("tenant-b", &[], all_start, all_end)
            .await
            .unwrap();
        check!(names.contains(&"region".to_string()), "got: {names:?}");
        check!(
            !names.contains(&"service_name".to_string()),
            "tenant-a's label must not leak"
        );

        let values = store
            .label_values("tenant-a", "service_name", &[], all_start, all_end)
            .await
            .unwrap();
        check!(values == vec!["api".to_string()], "got: {values:?}");

        let types = store
            .profile_types("tenant-a", all_start, all_end)
            .await
            .unwrap();
        check!(!types.is_empty(), "tenant-a has a profile type");
        check!(
            store
                .profile_types("tenant-c", all_start, all_end)
                .await
                .unwrap()
                == Vec::<String>::new(),
            "an unknown tenant has none"
        );

        let series = store
            .series("tenant-a", &[], &[], all_start, all_end)
            .await
            .unwrap();
        check!(series.len() == 1, "got: {series:?}");

        // The window is honoured at both ends: tenant-a's sample sits at
        // 1000ms, so a window that stops before it or starts after it is
        // empty. A delegation that swapped the ends would answer differently.
        check!(
            store
                .series("tenant-a", &[], &[], 0, 500)
                .await
                .unwrap()
                .is_empty(),
            "before"
        );
        check!(
            store
                .series("tenant-a", &[], &[], 2_000, 10_000)
                .await
                .unwrap()
                .is_empty(),
            "after"
        );
        check!(
            !store
                .series("tenant-a", &[], &[], 900, 1_100)
                .await
                .unwrap()
                .is_empty(),
            "around"
        );
    }

    #[tokio::test]
    async fn appended_wal_records_are_queryable_as_hot_profiles() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let flamegraph = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(flamegraph.total == 9);
        assert!(flamegraph.names.iter().any(|name| name == "hot_fn"));
    }

    #[tokio::test]
    async fn appended_wal_records_are_queryable_with_millisecond_timestamps() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let series = engine
            .select_series(
                ("tenant-a", PT, r#"{service_name="api"}"#),
                &[],
                secs(1),
                SeriesAgg::Sum,
                (0, i64::MAX),
            )
            .await
            .unwrap();

        assert!(series.len() == 1);
        assert!(series[0].points == vec![(1_700_000, 9.0)]);
    }

    #[tokio::test]
    async fn appended_wal_records_preserve_trace_ids_in_hot_samples() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();

        let scan = store
            .select(
                "tenant-a",
                PT,
                &[LabelMatcher::new(
                    "service_name".to_string(),
                    MatchOp::Eq,
                    "api".to_string(),
                )],
                0,
                i64::MAX,
            )
            .await
            .unwrap();
        let batches = scan
            .ctx
            .sql(&format!(
                "SELECT {PCOL_TRACE_ID} FROM {}",
                scan.samples_table
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let trace_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(!trace_ids.is_null(0));
        assert!(trace_ids.value(0) == &[0xaa; 16]);
    }

    fn record_at(value: i64, timestamp_ns: i64) -> ProfileRecord {
        let mut rec = record();
        rec.samples[0].value = value;
        rec.samples[0].timestamp_ns = timestamp_ns;
        rec
    }

    #[tokio::test]
    async fn retention_evicts_samples_older_than_the_horizon() {
        // Tight 1s window: an old sample must be dropped once a much newer one
        // arrives, so the hot store does not grow without bound.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: secs(1),
            max_records: usize::MAX,
        });
        // Old sample at t=0ms, then a fresh sample 10s later.
        store.append_record(record_at(5, 0)).unwrap();
        store
            .append_record(record_at(7, 10_000 * 1_000_000))
            .unwrap();

        // Querying the full range must see only the surviving fresh sample.
        let stats = store.stats("tenant-a", 0, i64::MAX).await.unwrap();
        assert!(stats.oldest_profile_time == Some(10_000), "{stats:?}");
        assert!(stats.newest_profile_time == Some(10_000), "{stats:?}");

        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        assert!(fg.total == 7, "old sample not evicted: {}", fg.total);
    }

    #[tokio::test]
    async fn retention_evicts_by_record_budget() {
        // max_records=2 with an unbounded age window: the third append drops the
        // oldest record regardless of age.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: unlimited_max_age(),
            max_records: 2,
        });
        store.append_record(record_at(1, 1_000_000)).unwrap();
        store.append_record(record_at(2, 2_000_000)).unwrap();
        store.append_record(record_at(4, 3_000_000)).unwrap();

        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        // Only the two most recent records (values 2 and 4) survive.
        assert!(fg.total == 6, "budget eviction wrong: {}", fg.total);
    }

    #[tokio::test]
    async fn amortized_eviction_preserves_recent_query_results() {
        // Small budget + many appends: rebuilds are amortized (deferred), so the
        // queryable store may briefly over-retain already-evicted rows. A
        // timestamp-scoped query must still return exactly the records inside the
        // requested window, regardless of any lingering older rows.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: unlimited_max_age(),
            max_records: 10,
        });
        for i in 1..=50_i64 {
            store
                .append_record(record_at(i, i * 1_000_000_000))
                .unwrap();
        }
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        // Records 46..=50 sit at 46_000..=50_000 ms; query that window only.
        let fg = engine
            .select_merge_stacktraces(
                "tenant-a",
                PT,
                r#"{service_name="api"}"#,
                46_000,
                i64::MAX,
                0,
            )
            .await
            .unwrap();
        assert!(
            fg.total == 46 + 47 + 48 + 49 + 50,
            "recent-window query wrong: {}",
            fg.total
        );
    }

    #[tokio::test]
    async fn copy_on_write_snapshot_is_isolated_from_later_appends() {
        // A snapshot taken before an append must not observe the appended sample:
        // proves queries read a consistent COW snapshot rather than a live store.
        let store = super::WalTailProfileStore::new();
        store.append_record(record_at(5, 1_000_000)).unwrap();
        let snapshot = store.snapshot().unwrap();

        // Mutate the store after taking the snapshot.
        store.append_record(record_at(11, 2_000_000)).unwrap();

        // The pre-append snapshot still sees only the original sample.
        let before = snapshot.stats("tenant-a", 0, i64::MAX).await.unwrap();
        assert!(before.oldest_profile_time == Some(1), "{before:?}");
        assert!(before.newest_profile_time == Some(1), "{before:?}");

        // A fresh snapshot sees both samples.
        let after = store
            .snapshot()
            .unwrap()
            .stats("tenant-a", 0, i64::MAX)
            .await
            .unwrap();
        assert!(after.newest_profile_time == Some(2), "{after:?}");
    }
}

mod apply_record;
mod default_max_age;
mod default_max_records;
mod rebuild_amortize_factor;
mod retained;
mod retained_state;
mod retention_config;
mod run_wal_tail;
mod run_wal_tail_with_topic;
mod wal_tail_profile_store;

use apply_record::apply_record;
use default_max_age::DEFAULT_MAX_AGE;
use default_max_records::DEFAULT_MAX_RECORDS;
use rebuild_amortize_factor::REBUILD_AMORTIZE_FACTOR;
use retained::Retained;
use retained_state::RetainedState;
pub use retention_config::RetentionConfig;
pub use run_wal_tail::run_wal_tail;
pub use run_wal_tail_with_topic::run_wal_tail_with_topic;
pub use wal_tail_profile_store::WalTailProfileStore;
