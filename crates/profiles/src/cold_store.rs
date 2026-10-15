//! Object-store backed cold-block `ProfileStore`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    datatypes::{Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
#[cfg(test)]
use krabka_blockstore::OffsetRange;
use krabka_blockstore::{LabelMatcher, ProfileIndex, SeriesFingerprint};
use krabka_pprof::{
    ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, Frame,
    LazySymbolizer, NativeResolver, ProfileError, ProfileQueryStats, ProfileScan, ProfileStats,
    ProfileStore, SymbolDb, SymbolSource, profile_samples_schema,
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
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_blockstore::{BlockIndex, Labels, MatchOp, OffsetRange};
    use krabka_pprof::{
        DebuginfodConfig, EngineOpts, FlameEngine, ProfileIdentity, ProfileLabel,
        StacktraceIdentity,
    };
    use krabka_units::{mebibytes, millis, secs};
    use object_store::{
        ObjectStore,
        memory::InMemory,
        throttle::{ThrottleConfig, ThrottledStore},
    };

    use crate::hot_store::WalTailProfileStore;

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
            map(0, &[7, 3, 9]) == maplit::btreemap! { 3 => first, 7 => first + 1, 9 => first + 2 },
            "sorted by stored id, numbered from zero"
        );

        // The next block gets the next namespace, so the same stored id maps
        // somewhere else entirely.
        let second: u64 = 2 << 32;
        check!(
            map(1, &[7, 3, 9])
                == maplit::btreemap! { 3 => second, 7 => second + 1, 9 => second + 2 }
        );

        // Duplicates collapse rather than consuming a slot each.
        check!(map(0, &[5, 5, 5]) == maplit::btreemap! { 5 => 1_u64 << 32 });

        // A block that stored no partitions still gets the default one.
        check!(map(0, &[]) == maplit::btreemap! { super::STACKTRACE_PARTITION => 1_u64 << 32 });
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
        test_support::{
            CpuRecord, build_test_block, check_falls_back_to_address_frame, cold_api_flamegraph,
            cpu_record, index_with_series, serve_on_loopback_with,
        },
        wal::{ProfileRecord, WalSample},
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
    async fn query_analysis_measures_complete_blocks_and_separates_selected_series() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        api.samples.push(WalSample {
            value: 3,
            ..api.samples[0].clone()
        });
        let worker = record_at("t", "worker", vec![0], 7, 1_000_000_000);
        let later = record_at("t", "later", vec![0], 11, 5_000_000_000);
        let stranger = record_at("u", "secret", vec![0], 13, 1_000_000_000);
        let mut index = ProfileIndex::new();
        let mut planned_bytes = 0;
        let mut planned_symbols = 0;
        for BlockRecordsFixture { offset, records } in [
            BlockRecordsFixture {
                offset: 0,
                records: vec![api.clone(), worker],
            },
            BlockRecordsFixture {
                offset: 1,
                records: vec![later],
            },
            BlockRecordsFixture {
                offset: 2,
                records: vec![stranger],
            },
        ] {
            let tenant = &records[0].tenant;
            let metas = build_block(
                &store,
                tenant,
                0,
                &records,
                OffsetRange::new(offset, offset),
                &krabka_blockstore::ObjectStoreMetrics::unregistered(),
            )
            .await
            .unwrap();
            for rec in &records {
                let labels = Labels::from_pairs(rec.labels.iter().cloned());
                index
                    .add_series(tenant, labels.fingerprint(), &labels)
                    .unwrap();
            }
            for meta in metas {
                if offset == 0 {
                    planned_bytes += store
                        .head(&Path::from(meta.object_key.clone()))
                        .await
                        .unwrap()
                        .size;
                    planned_symbols += store
                        .head(&Path::from(format!("{}.symdb", meta.object_key)))
                        .await
                        .unwrap()
                        .size;
                }
                index.add_block(&meta);
                index.add_profile_block(tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
            }
        }
        let cold = ColdProfileStore::new(Arc::clone(&store), Arc::new(index));
        let matching = vec![LabelMatcher::new("service_name", MatchOp::Eq, "api")];
        let analysis = cold
            .query_stats("t", PT, &matching, 900, 1100)
            .await
            .unwrap();
        check!(analysis.fingerprints.len() == 1);
        check!(analysis.block_count == 1);
        check!(analysis.profile_count == 2);
        check!(analysis.sample_count == 3);
        let scope = &analysis.scopes[0];
        check!(scope.component_type == "Long term storage");
        check!(scope.component_count == 1);
        check!(scope.series_count == 2);
        check!(scope.index_bytes > 0);
        check!(scope.profile_bytes > 0);
        check!(scope.index_bytes + scope.profile_bytes == planned_bytes);
        check!(scope.symbol_bytes == planned_symbols);
        check!(!analysis.deduplication_needed);
        let missing = vec![LabelMatcher::new("service_name", MatchOp::Eq, "missing")];
        let excluded = cold
            .query_stats("t", PT, &missing, 900, 1100)
            .await
            .unwrap();
        check!(excluded.fingerprints.is_empty());
        check!(excluded.scopes == analysis.scopes);
        let out_of_range = cold.query_stats("t", PT, &[], 6000, 7000).await.unwrap();
        check!(out_of_range.block_count == 0);
        check!(out_of_range.sample_count == 0);
        check!(out_of_range.scopes[0].profile_bytes == 0);
        let empty = cold
            .query_stats("absent", PT, &[], 0, i64::MAX)
            .await
            .unwrap();
        check!(empty.block_count == 0);
        let mut hot = krabka_pprof::InMemoryProfileStore::new();
        hot.push_sample(
            ProfileIdentity::new("t", PT),
            api.labels
                .iter()
                .map(|label| ProfileLabel::new(label.name.clone(), label.value.clone()))
                .collect(),
            StacktraceIdentity::new(0, 0),
            5,
            1000,
        );
        let union = krabka_pprof::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
        let overlap = union
            .query_stats("t", PT, &matching, 900, 1100)
            .await
            .unwrap();
        check!(overlap.deduplication_needed);
        check!(overlap.fingerprints.len() == 1);
        check!(overlap.scopes.len() == 2);
        check!(overlap.block_count == 2);
        check!(overlap.sample_count == 4);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn query_analysis_with_omitted_bounds_returns_empty_for_hot_and_cold_data() {
        use krabka_pprof::{InMemoryProfileStore, UnionProfileStore};

        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut records = [
            record_at("t", "shared", vec![0], 5, 1_000_000_000),
            record_at("t", "cold-only", vec![0], 7, 1_000_000_000),
            record_at("t", "cold-only", vec![0], 11, 1_000_000_000),
        ];
        let profile_ids = ["shared-cold", "cold-a", "cold-b"];
        for index in 0..records.len() {
            records[index]
                .labels
                .push(krabka_blockstore::LabelPair::new(
                    "__profile_id__",
                    profile_ids[index],
                ));
        }
        let mut index = index_with_series(&records, &[]);
        for block in build_block(
            &objects,
            "t",
            0,
            &records,
            OffsetRange::new(0, 1),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap()
        {
            index.add_block(&block);
            index.add_profile_block("t", &block.object_key, vec![STACKTRACE_PARTITION]);
        }
        let cold = Arc::new(ColdProfileStore::new(objects, Arc::new(index)));
        let mut first = InMemoryProfileStore::new();
        let mut first_shared = records[0]
            .labels
            .iter()
            .map(|label| ProfileLabel::new(label.name.clone(), label.value.clone()))
            .collect::<Vec<_>>();
        first_shared
            .iter_mut()
            .find(|label| label.name == "__profile_id__")
            .unwrap()
            .value = "head-a".into();
        first.push_sample(
            ProfileIdentity::new("t", PT),
            first_shared,
            StacktraceIdentity::new(0, 0),
            5,
            1000,
        );
        let hot_labels = vec![
            ProfileLabel::new("service_name", "hot-only"),
            ProfileLabel::new(krabka_blockstore::LABEL_PROFILE_TYPE, PT),
        ];
        first.push_sample(
            ProfileIdentity::new("t", PT),
            hot_labels.clone(),
            StacktraceIdentity::new(0, 0),
            11,
            1000,
        );
        let mut second = InMemoryProfileStore::new();
        let mut second_shared = records[0]
            .labels
            .iter()
            .map(|label| ProfileLabel::new(label.name.clone(), label.value.clone()))
            .collect::<Vec<_>>();
        second_shared
            .iter_mut()
            .find(|label| label.name == "__profile_id__")
            .unwrap()
            .value = "head-b".into();
        second.push_sample(
            ProfileIdentity::new("t", PT),
            second_shared,
            StacktraceIdentity::new(0, 0),
            5,
            1000,
        );
        let mut wall_labels = hot_labels;
        wall_labels
            .iter_mut()
            .find(|label| label.name == krabka_blockstore::LABEL_PROFILE_TYPE)
            .unwrap()
            .value = "wall:wall:nanoseconds:wall:nanoseconds".into();
        second.push_sample(
            ProfileIdentity::new("t", "wall:wall:nanoseconds:wall:nanoseconds"),
            wall_labels,
            StacktraceIdentity::new(0, 0),
            13,
            2000,
        );
        second.push_sample(
            ProfileIdentity::new("u", PT),
            vec![ProfileLabel::new("service_name", "secret")],
            StacktraceIdentity::new(0, 0),
            17,
            1000,
        );
        let heads = Arc::new(UnionProfileStore::new(Arc::new(first), Arc::new(second)));
        let mixed = Arc::new(UnionProfileStore::new(heads, Arc::clone(&cold)));
        let server = serve_analysis_querier(mixed).await;
        let bound = server.address;
        let stop = server.shutdown;
        for TimeRange { start_ms, end_ms } in [
            TimeRange {
                start_ms: 0,
                end_ms: 0,
            },
            TimeRange {
                start_ms: 0,
                end_ms: 50,
            },
            TimeRange {
                start_ms: 3000,
                end_ms: 0,
            },
        ] {
            for AnalyzeQueryCase { tenant, query } in [
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{service_name=\"hot-only\"}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"hot-only\"}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{service_name=\"shared\"}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{__profile_id__=\"head-a\"}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{service_name=\"cold-only\"}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{service_name=\"missing\"}",
                },
                AnalyzeQueryCase {
                    tenant: "u",
                    query: "{service_name=\"secret\"}",
                },
                AnalyzeQueryCase {
                    tenant: "u",
                    query: "{service_name=\"hot-only\"}",
                },
                AnalyzeQueryCase {
                    tenant: "absent",
                    query: "{}",
                },
                AnalyzeQueryCase {
                    tenant: "t",
                    query: "{invalid",
                },
            ] {
                let response = analyze_query(
                    bound,
                    &AnalyzeQuery {
                        tenant,
                        query,
                        start_ms,
                        end_ms,
                    },
                )
                .await;
                check!(response == serde_json::json!({}));
            }
        }
        let _ = stop.send(());
        // Verify that the cold backend contains populated metadata and physical rows.
        check!(cold.series("t", &[], &[], 0, i64::MAX).await.unwrap().len() == 3);
        let analysis = cold.query_stats("t", "", &[], 900, 1100).await.unwrap();
        check!(analysis.fingerprints.len() == 2);
        check!(analysis.profile_count == 3);
        check!(analysis.sample_count == 3);
        let flushed = Arc::new(UnionProfileStore::new(
            Arc::new(InMemoryProfileStore::new()),
            cold,
        ));
        let server = serve_analysis_querier(flushed).await;
        let bound = server.address;
        let stop = server.shutdown;
        for TimeRange { start_ms, end_ms } in [
            TimeRange {
                start_ms: 0,
                end_ms: 0,
            },
            TimeRange {
                start_ms: 0,
                end_ms: 2000,
            },
            TimeRange {
                start_ms: 3000,
                end_ms: 0,
            },
        ] {
            let response = analyze_query(
                bound,
                &AnalyzeQuery {
                    tenant: "t",
                    query: "{}",
                    start_ms,
                    end_ms,
                },
            )
            .await;
            check!(response == serde_json::json!({}));
        }
        let _ = stop.send(());
    }

    /// Serves `store` on loopback as a querier with query analysis enabled.
    async fn serve_analysis_querier<S: krabka_pprof::ProfileStore + 'static>(
        store: Arc<S>,
    ) -> crate::test_support::BoundTestServer {
        use crate::query::{QuerierState, serve};

        let state = Arc::new(QuerierState::new(store).with_query_analysis_series_enabled(true));
        serve_on_loopback_with(async |addr, security, shutdown| {
            serve(addr, state, security, shutdown).await
        })
        .await
    }

    /// One `AnalyzeQuery` call: the tenant it is made as, and its body.
    struct AnalyzeQuery<'a> {
        tenant: &'a str,
        query: &'a str,
        start_ms: i64,
        end_ms: i64,
    }

    struct AnalyzeQueryCase {
        tenant: &'static str,
        query: &'static str,
    }

    struct TimeRange {
        start_ms: i64,
        end_ms: i64,
    }

    struct RecordOffsetValue {
        offset: u64,
        value: i64,
    }

    struct BlockRecordsFixture {
        offset: i64,
        records: Vec<ProfileRecord>,
    }

    /// Posts `request` to the querier at `bound` and returns the JSON answer,
    /// failing on any non-success status.
    async fn analyze_query(
        bound: std::net::SocketAddr,
        request: &AnalyzeQuery<'_>,
    ) -> serde_json::Value {
        reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/AnalyzeQuery"
            ))
            .header("x-scope-orgid", request.tenant)
            .json(&serde_json::json!({
                "query": request.query,
                "start": request.start_ms,
                "end": request.end_ms,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_cached_query_replans_compacted_blocks_and_preserves_missing_data_errors() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        let mut input_keys = Vec::new();
        for RecordOffsetValue { offset, value } in [
            RecordOffsetValue {
                offset: 0,
                value: 5,
            },
            RecordOffsetValue {
                offset: 1,
                value: 7,
            },
        ] {
            let rec = record("t", "api", vec![0], value);
            let meta = build_test_block(
                &store,
                std::slice::from_ref(&rec),
                i64::try_from(offset).unwrap(),
            )
            .await;
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index
                .add_series("t", labels.fingerprint(), &labels)
                .unwrap();
            index.add_block(&meta);
            index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
            input_keys.push(meta.object_key);
        }
        let snapshot = "index/profiles.json";
        index.save_latest_snapshot(&store, snapshot).await.unwrap();
        let cold = Arc::new(
            ColdProfileStore::new(
                Arc::clone(&store),
                Arc::new(
                    ProfileIndex::load_latest_snapshot(&store, snapshot)
                        .await
                        .unwrap(),
                ),
            )
            .with_index_snapshot(
                snapshot.to_string(),
                krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_MAX,
            ),
        );
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let before = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        check!(before.total == 12);

        let output = crate::compactor::compact_blocks(
            &store,
            &mut index,
            "t",
            &input_keys,
            "blocks/t/compacted.parquet",
        )
        .await
        .unwrap();
        index.save_latest_snapshot(&store, snapshot).await.unwrap();
        for key in input_keys {
            store
                .delete(&Path::from(format!("{key}.symdb")))
                .await
                .unwrap();
            store.delete(&Path::from(key)).await.unwrap();
        }
        let after = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        check!(after.total == before.total);
        check!(after.names == before.names);

        // The published index still names this output: its loss is an error.
        store.delete(&Path::from(output.object_key)).await.unwrap();
        assert!(
            engine
                .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0,)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cold_store_merges_blocks_with_local_symbol_partitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", vec![0], 5);
        let rec_b = record("t", "api", vec![0], 7);
        let meta_a = build_test_block(&store, std::slice::from_ref(&rec_a), 0).await;
        let meta_b = build_test_block(&store, std::slice::from_ref(&rec_b), 1).await;
        let index = index_with_series([&rec_a], &[&meta_a, &meta_b]);

        let fg = cold_api_flamegraph(store, index).await;

        assert!(fg.total == 12);
        assert!(fg.names.iter().any(|name| name == "main"));
    }

    async fn single_block_cold_store(records: &[ProfileRecord]) -> ColdProfileStore {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = build_test_block(&store, records, 0).await;
        let index = index_with_series(records, &[&meta]);
        ColdProfileStore::new(store, Arc::new(index))
    }

    async fn api_worker_cold_store() -> ColdProfileStore {
        let records = [
            record_at("t", "api", vec![0], 5, 1_000_000_000),
            record_at("t", "worker", vec![0], 7, 3_000_000_000),
        ];
        single_block_cold_store(&records).await
    }

    #[tokio::test]
    async fn cold_store_projects_labels_with_matchers() {
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

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
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

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
        let first = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let later = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![first, later];
        let cold = single_block_cold_store(&records).await;

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
        let meta_early = build_test_block(&store, std::slice::from_ref(&early), 0).await;
        let meta_late = build_test_block(&store, std::slice::from_ref(&late), 1).await;
        assert!(meta_early.min_ts == 1000 && meta_early.max_ts == 1000);
        assert!(meta_late.min_ts == 5000 && meta_late.max_ts == 5000);
        let index = index_with_series([&early, &late], &[&meta_early, &meta_late]);
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
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

        let types = cold.profile_types("t", 2_000, 3_000).await.unwrap();

        assert!(types.is_empty(), "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_profile_types_do_not_leak_types_outside_time_range_in_same_block() {
        let cpu = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut memory = record_at("t", "api", vec![0], 7, 3_000_000_000);
        memory.profile_type = "memory:alloc_space:bytes:space:bytes".to_string();
        memory.labels = vec![
            krabka_blockstore::LabelPair::new("__name__", "memory"),
            krabka_blockstore::LabelPair::new("__profile_type__", &memory.profile_type),
            krabka_blockstore::LabelPair::new("service_name", "api"),
        ];
        let records = vec![cpu, memory];
        let cold = single_block_cold_store(&records).await;

        let types = cold.profile_types("t", 1_000, 1_000).await.unwrap();

        assert!(types == vec![PT.to_string()], "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_honor_query_time_range() {
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

        let values = cold
            .label_values("t", "service_name", &[], 2_000, 3_000)
            .await
            .unwrap();

        assert!(values.is_empty(), "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_do_not_leak_series_outside_time_range_in_same_block() {
        let cold = api_worker_cold_store().await;
        let values = cold
            .label_values("t", "service_name", &[], 1_000, 1_000)
            .await
            .unwrap();

        assert!(values == vec!["api".to_string()], "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_honor_query_time_range() {
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

        let names = cold.label_names("t", &[], 2_000, 3_000).await.unwrap();

        assert!(names.is_empty(), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_do_not_leak_series_outside_time_range_in_same_block() {
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        worker
            .labels
            .push(krabka_blockstore::LabelPair::new("pod", "worker-0"));
        let records = vec![api, worker];
        let cold = single_block_cold_store(&records).await;

        let names = cold.label_names("t", &[], 1_000, 1_000).await.unwrap();

        assert!(!names.contains(&"pod".to_string()), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_series_honor_query_time_range() {
        let rec = record("t", "api", vec![0], 5);
        let cold = single_block_cold_store(std::slice::from_ref(&rec)).await;

        let series = cold
            .series("t", &[], &["service_name".to_string()], 2_000, 3_000)
            .await
            .unwrap();

        assert!(series.is_empty(), "{series:?}");
    }

    #[tokio::test]
    async fn cold_store_series_do_not_leak_series_outside_time_range_in_same_block() {
        let cold = api_worker_cold_store().await;
        let series = cold
            .series("t", &[], &["service_name".to_string()], 1_000, 1_000)
            .await
            .unwrap();

        assert!(
            series == vec![vec![ProfileLabel::new("service_name", "api")]],
            "{series:?}"
        );
    }

    #[tokio::test]
    async fn cold_store_unbounded_label_metadata_uses_index_without_loading_blocks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_test_block(&store, &records, 0).await;
        let index = index_with_series(&records, &[&meta]);
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
                    vec![ProfileLabel::new("service_name", "api")],
                    vec![ProfileLabel::new("service_name", "worker")]
                ],
            "{series:?}"
        );
    }

    #[test]
    fn cold_store_native_resolver_falls_back_to_address_frame() {
        check_falls_back_to_address_frame(local_native_resolver().as_ref());
    }

    #[test]
    fn filter_and_remap_batch_selects_and_remaps_in_one_pass() {
        use krabka_blockstore::{ProfileSampleFields, ProfileSampleRow, encode_profile_samples};

        let fp_keep = 7_u64;
        let fp_drop = 99_u64;
        let rows = vec![
            // keep: matching fp, type, in range; partition 0
            ProfileSampleRow {
                timestamp: 1_000,
                fields: ProfileSampleFields {
                    series_fingerprint: fp_keep,
                    profile_type: PT.to_string(),
                    stacktrace_id: 1,
                    value: 10,
                    stacktrace_partition: 0,
                    total_value: 10,
                    span_id: None,
                    trace_id: None,
                    wal_sample_ids: Vec::new(),
                },
            },
            // drop: wrong fingerprint
            ProfileSampleRow {
                timestamp: 1_000,
                fields: ProfileSampleFields {
                    series_fingerprint: fp_drop,
                    profile_type: PT.to_string(),
                    stacktrace_id: 2,
                    value: 5,
                    stacktrace_partition: 0,
                    total_value: 5,
                    span_id: None,
                    trace_id: None,
                    wal_sample_ids: Vec::new(),
                },
            },
            // drop: out of time range
            ProfileSampleRow {
                timestamp: 9_999,
                fields: ProfileSampleFields {
                    series_fingerprint: fp_keep,
                    profile_type: PT.to_string(),
                    stacktrace_id: 3,
                    value: 5,
                    stacktrace_partition: 0,
                    total_value: 5,
                    span_id: None,
                    trace_id: None,
                    wal_sample_ids: Vec::new(),
                },
            },
            // keep: matching, distinct partition 1 to verify per-row remap
            ProfileSampleRow {
                timestamp: 2_000,
                fields: ProfileSampleFields {
                    series_fingerprint: fp_keep,
                    profile_type: PT.to_string(),
                    stacktrace_id: 4,
                    value: 20,
                    stacktrace_partition: 1,
                    total_value: 20,
                    span_id: None,
                    trace_id: None,
                    wal_sample_ids: Vec::new(),
                },
            },
        ];
        let batch = encode_profile_samples(&rows).unwrap();

        let partition_base = 1_u64 << 32;
        // Dense per-block map: stored partitions {0, 1} -> {base|0, base|1}.
        let mut partition_map = BTreeMap::new();
        partition_map.insert(0_u64, partition_base);
        partition_map.insert(1_u64, partition_base | 1);
        let fps = BTreeSet::from([fp_keep]);
        let out = filter_and_remap_batch(
            &batch,
            &partition_map,
            BlockRowFilter {
                fps: &fps,
                profile_type: PT,
                start_ms: 0,
                end_ms: 5_000,
            },
        )
        .unwrap();

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
        cpu_record(CpuRecord {
            tenant,
            service,
            stack,
            value,
            timestamp_ns,
            function: "main",
        })
    }

    #[tokio::test]
    async fn many_cold_blocks_keep_their_symbols_and_fail_as_a_whole_if_one_is_missing() {
        // Awaiting reads makes block loads interleave. More than four blocks
        // exercise several waves, with the same local partition in every block.
        let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: Duration::from_millis(1),
                ..ThrottleConfig::default()
            },
        ));
        let records = (1..=9)
            .map(|value| {
                let mut record = record("t", "api", vec![0], value);
                record.symbols.strings[1] = format!("frame-{value}");
                record
            })
            .collect::<Vec<_>>();
        let hot = Arc::new(WalTailProfileStore::new());
        hot.append_records(records.clone()).unwrap();
        let control = FlameEngine::new(hot, EngineOpts::default())
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        check!(control.total == 45);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(records[0].labels.iter().cloned());
        index
            .add_series("t", labels.fingerprint(), &labels)
            .unwrap();
        let mut keys = Vec::new();
        for record_entry in records.iter().enumerate() {
            let offset = i64::try_from(record_entry.0).unwrap();
            let record = record_entry.1;
            let meta = build_block(
                &store,
                "t",
                0,
                std::slice::from_ref(record),
                OffsetRange::new(offset, offset),
                &krabka_blockstore::ObjectStoreMetrics::unregistered(),
            )
            .await
            .unwrap()
            .remove(0);
            keys.push(meta.object_key.clone());
            index.add_block(&meta);
            index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
        }
        let cold = Arc::new(ColdProfileStore::new(Arc::clone(&store), Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let result = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        check!(result == control);

        // A failed load must not publish a flamegraph from the other eight blocks.
        store.delete(&Path::from(keys[4].clone())).await.unwrap();
        check!(
            engine
                .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
                .await
                .is_err()
        );
    }
}
mod block_partition_map;
mod cold_profile_store;
mod composite_symbols;
mod filter_and_remap_batch;
use block_partition_map::block_partition_map;
pub use cold_profile_store::ColdProfileStore;
use composite_symbols::CompositeSymbols;
use filter_and_remap_batch::{BlockRowFilter, filter_and_remap_batch};

pub(crate) fn batch_fingerprints_overlap(
    batch: &RecordBatch,
    fps: &BTreeSet<SeriesFingerprint>,
) -> bool {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    (0..batch.num_rows()).any(|row| fps.contains(&fingerprints.value(row)))
}

pub(crate) fn is_unbounded_metadata_range(start_ms: i64, end_ms: i64) -> bool {
    start_ms == 0 && end_ms == i64::MAX
}

pub(crate) fn local_native_resolver() -> Arc<ChainedResolver> {
    Arc::new(ChainedResolver::new(vec![
        Arc::new(FileSystemResolver::default()),
        Arc::new(AddressFallbackResolver),
    ]))
}
