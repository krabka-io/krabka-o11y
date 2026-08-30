//! Object-store backed cold-block `ProfileStore`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    datatypes::{Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::{LabelMatcher, ProfileIndex, SeriesFingerprint};
use krabka_pprof::{
    ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, Frame,
    LazySymbolizer, NativeResolver, ProfileError, ProfileScan, ProfileStats, ProfileStore,
    SymbolDb, SymbolSource, profile_samples_schema,
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    blockbuilder::STACKTRACE_PARTITION,
    ids::{ExternalPartition, LocalPartition},
    symbolizer::AddressFallbackResolver,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_blockstore::{BlockIndex, Labels, MatchOp};
    use krabka_pprof::{DebuginfodConfig, EngineOpts, FlameEngine, SymbolizeRequest};
    use krabka_units::{mebibytes, millis, secs};
    use object_store::{ObjectStore, memory::InMemory};

    /// `block_partition_map` gives every block its own 32-bit namespace so
    /// stacktrace partition ids from different blocks cannot collide. The
    /// high half is the block's index plus one; the low half is the
    /// partition's position after sorting and deduplication, not the value
    /// the block stored.
    #[test]
    fn partition_ids_are_namespaced_by_block_and_renumbered_densely() {
        let map =
            |block_idx, stored: &[u64]| super::block_partition_map(block_idx, stored).unwrap();

        // Block 0 occupies the namespace at 1 << 32; the stored ids keep
        // their identity as keys but are renumbered densely as values.
        let first: u64 = 1 << 32;
        check!(
            map(0, &[7, 3, 9])
                == std::collections::BTreeMap::from([(3, first), (7, first + 1), (9, first + 2),]),
            "sorted by stored id, numbered from zero"
        );

        // The next block gets the next namespace, so the same stored id maps
        // somewhere else entirely.
        let second: u64 = 2 << 32;
        check!(
            map(1, &[7, 3, 9])
                == std::collections::BTreeMap::from([
                    (3, second),
                    (7, second + 1),
                    (9, second + 2),
                ])
        );

        // Duplicates collapse rather than consuming a slot each.
        check!(map(0, &[5, 5, 5]) == std::collections::BTreeMap::from([(5, 1_u64 << 32)]));

        // A block that stored no partitions still gets the default one.
        check!(
            map(0, &[])
                == std::collections::BTreeMap::from([(super::STACKTRACE_PARTITION, 1_u64 << 32)])
        );
    }

    /// A metadata query covering all of time is answered from the index alone,
    /// so the check has to be exact at both ends.
    #[test]
    fn only_the_full_range_counts_as_unbounded() {
        check!(super::is_unbounded_metadata_range(0, i64::MAX));

        check!(
            !super::is_unbounded_metadata_range(1, i64::MAX),
            "a later start is bounded"
        );
        check!(
            !super::is_unbounded_metadata_range(-1, i64::MAX),
            "so is an earlier one"
        );
        check!(
            !super::is_unbounded_metadata_range(0, i64::MAX - 1),
            "an earlier end is bounded"
        );
        check!(
            !super::is_unbounded_metadata_range(0, 0),
            "and so is an empty range"
        );
    }

    use super::*;
    use crate::{
        blockbuilder::build_block,
        wal::{ProfileRecord, WalSample, WalSymbolSet},
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[test]
    fn cold_store_accepts_explicit_debuginfod_config() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();

        ColdProfileStore::new_with_debuginfod_config(
            store,
            Arc::new(ProfileIndex::new()),
            vec!["http://127.0.0.1:1".to_string()],
            config,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn cold_store_merges_blocks_with_local_symbol_partitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", vec![0], 5);
        let rec_b = record("t", "api", vec![0], 7);
        let meta_a = build_block(&store, "t", 0, std::slice::from_ref(&rec_a), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_b), (1, 1))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec_a.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta_a);
        index.add_block(&meta_b);
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());

        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(fg.total == 12);
        assert!(fg.names.iter().any(|name| name == "main"));
    }

    #[tokio::test]
    async fn cold_store_projects_labels_with_matchers() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Eq, "api")],
                0,
                i64::MAX,
            )
            .await
            .unwrap();

        assert!(values == vec!["api".to_string()]);
    }

    #[tokio::test]
    async fn cold_store_stats_report_block_time_bounds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 0, i64::MAX).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(1000),
                }
        );
    }

    #[tokio::test]
    async fn cold_store_stats_honor_sample_time_inside_overlapping_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let later = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![first.clone(), later.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&first, &later] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 1_000, 1_000).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(1000),
                }
        );
    }

    #[tokio::test]
    async fn cold_store_stats_aggregate_block_bounds_without_scanning_batches() {
        // Two blocks with disjoint, known time spans. The global stats must be the
        // min of the mins and the max of the maxes derived from the index's
        // per-block metadata. To prove the bounds come from block metadata and not
        // from scanning sample rows, the parquet block objects are DELETED from the
        // store before calling `stats`: a row scan would fail to load them, but the
        // index aggregate succeeds because it never touches the blocks.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let early = record_at("t", "api", vec![0], 5, 1_000_000_000); // 1000 ms
        let late = record_at("t", "worker", vec![0], 7, 5_000_000_000); // 5000 ms
        let meta_early = build_block(&store, "t", 0, std::slice::from_ref(&early), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_late = build_block(&store, "t", 0, std::slice::from_ref(&late), (1, 1))
            .await
            .unwrap()
            .remove(0);
        assert!(meta_early.min_ts == 1000 && meta_early.max_ts == 1000);
        assert!(meta_late.min_ts == 5000 && meta_late.max_ts == 5000);
        let mut index = ProfileIndex::new();
        for rec in [&early, &late] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta_early);
        index.add_block(&meta_late);
        // Drop the block payloads so any attempt to load+scan a batch would error.
        store
            .delete(&Path::from(meta_early.object_key.clone()))
            .await
            .unwrap();
        store
            .delete(&Path::from(meta_late.object_key.clone()))
            .await
            .unwrap();
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 0, i64::MAX).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(5000),
                }
        );

        // A tenant with no blocks reports no data without touching the store.
        let empty = stats_for_unknown_tenant(&cold).await;
        assert!(
            empty
                == ProfileStats {
                    data_ingested: false,
                    oldest_profile_time: None,
                    newest_profile_time: None,
                }
        );
    }

    async fn stats_for_unknown_tenant(cold: &ColdProfileStore) -> ProfileStats {
        cold.stats("absent-tenant", 0, i64::MAX).await.unwrap()
    }

    #[tokio::test]
    async fn cold_store_profile_types_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 2_000, 3_000).await.unwrap();

        assert!(types.is_empty(), "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_profile_types_do_not_leak_types_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cpu = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut memory = record_at("t", "api", vec![0], 7, 3_000_000_000);
        memory.profile_type = "memory:alloc_space:bytes:space:bytes".to_string();
        memory.labels = vec![
            ("__name__".to_string(), "memory".to_string()),
            ("__profile_type__".to_string(), memory.profile_type.clone()),
            ("service_name".to_string(), "api".to_string()),
        ];
        let records = vec![cpu.clone(), memory.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 1_000, 1_000).await.unwrap();

        assert!(types == vec![PT.to_string()], "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values("t", "service_name", &[], 2_000, 3_000)
            .await
            .unwrap();

        assert!(values.is_empty(), "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values("t", "service_name", &[], 1_000, 1_000)
            .await
            .unwrap();

        assert!(values == vec!["api".to_string()], "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let names = cold.label_names("t", &[], 2_000, 3_000).await.unwrap();

        assert!(names.is_empty(), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        worker
            .labels
            .push(("pod".to_string(), "worker-0".to_string()));
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let names = cold.label_names("t", &[], 1_000, 1_000).await.unwrap();

        assert!(!names.contains(&"pod".to_string()), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_series_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let series = cold
            .series("t", &[], &["service_name".to_string()], 2_000, 3_000)
            .await
            .unwrap();

        assert!(series.is_empty(), "{series:?}");
    }

    #[tokio::test]
    async fn cold_store_series_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let series = cold
            .series("t", &[], &["service_name".to_string()], 1_000, 1_000)
            .await
            .unwrap();

        assert!(
            series == vec![vec![("service_name".to_string(), "api".to_string())]],
            "{series:?}"
        );
    }

    #[tokio::test]
    async fn cold_store_unbounded_label_metadata_uses_index_without_loading_blocks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        store
            .delete(&Path::from(meta.object_key.clone()))
            .await
            .unwrap();
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 0, i64::MAX).await.unwrap();
        let names = cold.label_names("t", &[], 0, i64::MAX).await.unwrap();
        let values = cold
            .label_values("t", "service_name", &[], 0, i64::MAX)
            .await
            .unwrap();
        let series = cold
            .series("t", &[], &["service_name".to_string()], 0, i64::MAX)
            .await
            .unwrap();

        check!(types == vec![PT.to_string()], "{types:?}");
        check!(names.contains(&"service_name".to_string()), "{names:?}");
        check!(
            values == vec!["api".to_string(), "worker".to_string()],
            "{values:?}"
        );
        check!(
            series
                == vec![
                    vec![("service_name".to_string(), "api".to_string())],
                    vec![("service_name".to_string(), "worker".to_string())]
                ],
            "{series:?}"
        );
    }

    #[test]
    fn cold_store_native_resolver_falls_back_to_address_frame() {
        let resolver = local_native_resolver();
        let out = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: "/missing/native".to_string(),
                address: 0x99,
            })
            .unwrap();

        assert!(out[0].function == "/missing/native+0x99");
        assert!(out[0].file == "/missing/native");
    }

    #[test]
    fn filter_and_remap_batch_selects_and_remaps_in_one_pass() {
        use krabka_blockstore::{ProfileSampleRow, encode_profile_samples};

        let fp_keep = 7_u64;
        let fp_drop = 99_u64;
        let rows = vec![
            // keep: matching fp, type, in range; partition 0
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 1_000,
                profile_type: PT.to_string(),
                stacktrace_id: 1,
                value: 10,
                stacktrace_partition: 0,
                total_value: 10,
                span_id: None,
                trace_id: None,
            },
            // drop: wrong fingerprint
            ProfileSampleRow {
                series_fingerprint: fp_drop,
                timestamp: 1_000,
                profile_type: PT.to_string(),
                stacktrace_id: 2,
                value: 5,
                stacktrace_partition: 0,
                total_value: 5,
                span_id: None,
                trace_id: None,
            },
            // drop: out of time range
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 9_999,
                profile_type: PT.to_string(),
                stacktrace_id: 3,
                value: 5,
                stacktrace_partition: 0,
                total_value: 5,
                span_id: None,
                trace_id: None,
            },
            // keep: matching, distinct partition 1 to verify per-row remap
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 2_000,
                profile_type: PT.to_string(),
                stacktrace_id: 4,
                value: 20,
                stacktrace_partition: 1,
                total_value: 20,
                span_id: None,
                trace_id: None,
            },
        ];
        let batch = encode_profile_samples(&rows).unwrap();

        let partition_base = 1_u64 << 32;
        // Dense per-block map: stored partitions {0, 1} -> {base|0, base|1}.
        let partition_map = BTreeMap::from([(0_u64, partition_base), (1_u64, partition_base | 1)]);
        let fps = BTreeSet::from([fp_keep]);
        let out = filter_and_remap_batch(&batch, &partition_map, &fps, PT, 0, 5_000).unwrap();

        // Two surviving rows (the partition-0 and partition-1 keeps).
        assert!(out.num_rows() == 2);
        let out_fps = out.column(0).as_primitive::<UInt64Type>();
        let out_partitions = out.column(5).as_primitive::<UInt64Type>();
        check!(out_fps.value(0) == fp_keep);
        check!(out_fps.value(1) == fp_keep);
        // Partitions remapped once over the whole batch: base|local preserved
        // per surviving row.
        check!(out_partitions.value(0) == partition_base);
        check!(out_partitions.value(1) == (partition_base | 1));
        // Schema matches the canonical samples schema (consumed by the MemTable).
        check!(out.schema() == profile_samples_schema());
    }

    fn record(tenant: &str, service: &str, stack: Vec<u32>, value: i64) -> ProfileRecord {
        record_at(tenant, service, stack, value, 1_000_000_000)
    }

    fn record_at(
        tenant: &str,
        service: &str,
        stack: Vec<u32>,
        value: i64,
        timestamp_ns: i64,
    ) -> ProfileRecord {
        ProfileRecord {
            tenant: tenant.to_string(),
            labels: vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("__profile_type__".to_string(), PT.to_string()),
                ("service_name".to_string(), service.to_string()),
            ],
            profile_type: PT.to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: stack,
                value,
                timestamp_ns,
                span_id: None,
                trace_id: None,
            }],
            symbols: symbols(),
        }
    }

    fn symbols() -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), "main".to_string()],
            functions: vec![crate::wal::WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![crate::wal::WalLocation {
                address: 0,
                mapping_id: 0,
                lines: vec![(0, 1)],
            }],
            mappings: Vec::new(),
        }
    }
}

// === split-modules: generated submodules ===
mod batch_fingerprints_overlap;
mod block_partition_map;
mod cold_profile_store;
mod composite_symbols;
mod filter_and_remap_batch;
mod is_unbounded_metadata_range;
mod local_native_resolver;

use batch_fingerprints_overlap::batch_fingerprints_overlap;
use block_partition_map::block_partition_map;
pub use cold_profile_store::ColdProfileStore;
use composite_symbols::CompositeSymbols;
use filter_and_remap_batch::filter_and_remap_batch;
use is_unbounded_metadata_range::is_unbounded_metadata_range;
use local_native_resolver::local_native_resolver;
