//! Hot/cold `ProfileStore` union.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    datatypes::UInt64Type,
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::LabelMatcher;

use crate::{
    Frame, PCOL_STACKTRACE_PARTITION, ProfileError, ProfileScan, ProfileStats, ProfileStore,
    SymbolSource, profile_samples_schema,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use datafusion::arrow::{array::AsArray, datatypes::UInt64Type};

    use crate::{
        EngineOpts, FlameEngine, FunctionRec, InMemoryProfileStore, LocationRec, ProfileStats,
        ProfileStore,
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn store_with_frame(frame: &str, value: i64, timestamp_ms: i64) -> InMemoryProfileStore {
        store_with_frame_partition(frame, value, timestamp_ms, 0)
    }

    fn store_with_frame_partition(
        frame: &str,
        value: i64,
        timestamp_ms: i64,
        partition: u64,
    ) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name = store.symbols_mut().intern_string(frame);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
        let location = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![crate::LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location]);
        store.push_sample(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (partition, stacktrace),
            value,
            timestamp_ms,
        );
        store
    }

    #[tokio::test]
    async fn hot_cold_union_merges_samples_without_raw_id_collision() {
        let hot = store_with_frame("hot", 7, 20);
        let cold = store_with_frame("cold", 5, 10);
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
        let engine = FlameEngine::new(Arc::new(union), EngineOpts::default());

        let flamegraph = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, 60_000, 0)
            .await
            .unwrap();

        check!(flamegraph.total == 12);
        check!(flamegraph.names.iter().any(|name| name == "hot"));
        check!(flamegraph.names.iter().any(|name| name == "cold"));
    }

    #[tokio::test]
    async fn hot_cold_union_merges_metadata_and_stats() {
        let mut hot = store_with_frame("hot", 7, 20);
        hot.push_sample(
            ("tenant-a", "memory:alloc_space:bytes:space:bytes"),
            vec![("service_name".to_string(), "worker".to_string())],
            (0, 1),
            3,
            40,
        );
        let cold = store_with_frame("cold", 5, 10);
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));

        let types = union.profile_types("tenant-a", 0, 100).await.unwrap();
        let values = union
            .label_values("tenant-a", "service_name", &[], 0, 100)
            .await
            .unwrap();
        let names = union.label_names("tenant-a", &[], 0, 100).await.unwrap();
        let series = union
            .series("tenant-a", &[], &["service_name".to_string()], 0, 100)
            .await
            .unwrap();
        let stats = union.stats("tenant-a", 0, 100).await.unwrap();

        check!(
            types
                == vec![
                    "memory:alloc_space:bytes:space:bytes".to_string(),
                    PT.to_string(),
                ]
        );
        check!(names == vec!["service_name".to_string()]);
        check!(values == vec!["api".to_string(), "worker".to_string()]);
        check!(series.len() == 2);
        check!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(10),
                    newest_profile_time: Some(40),
                }
        );
    }

    #[tokio::test]
    async fn hot_cold_union_stats_reports_data_when_only_one_side_has_samples() {
        let hot = store_with_frame("hot", 7, 20);
        let cold = InMemoryProfileStore::new();
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));

        let stats = union.stats("tenant-a", 0, 100).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(20),
                    newest_profile_time: Some(20),
                }
        );
    }

    #[tokio::test]
    async fn hot_partition_remap_preserves_existing_low_and_high_bits() {
        let partition = 0x0100_0000_0000_0001;
        let hot = store_with_frame_partition("hot", 7, 20, partition);
        let cold = InMemoryProfileStore::new();
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
        let scan = union.select("tenant-a", PT, &[], 0, 100).await.unwrap();
        let df = scan
            .ctx
            .sql(&format!(
                "SELECT {} FROM {}",
                crate::PCOL_STACKTRACE_PARTITION,
                scan.samples_table
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let partitions = out[0].column(0).as_primitive::<UInt64Type>();

        assert!(partitions.value(0) == partition);
    }
}

mod collect_and_remap;
mod max_option;
mod min_option;
mod remap_partitions;
mod sorted_union;
mod union_profile_store;
mod union_symbols;

use collect_and_remap::collect_and_remap;
use max_option::max_option;
use min_option::min_option;
use remap_partitions::remap_partitions;
use sorted_union::sorted_union;
pub use union_profile_store::UnionProfileStore;
use union_symbols::UnionSymbols;
