//! Profile block compaction.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryArray, UInt64Array},
    compute::concat_batches,
    datatypes::{Int32Type, Int64Type, SchemaRef, UInt64Type},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use krabka_blockstore::{
    BlockMeta, BlockStoreError, BlockStreamWriter, BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP,
    CompactionJob, CompactionPolicy, DEFAULT_BLOCK_READ_MAX, MERGE_BATCH_ROWS,
    MERGE_READ_BATCH_ROWS, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, ProfileIndex,
    ProfileSampleRow, SortedMerge, SummaryColumns, encode_profile_samples, input_key_fingerprint,
    open_block_stream, plan_compactions as plan_level_compactions, profile_samples_decl,
    versioned_compaction_key,
};
use krabka_pprof::SymbolDb;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use crate::{blockbuilder::STACKTRACE_PARTITION, error::ProfilesError};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_blockstore::{BlockIndex, BlockLevel, Labels};
    use krabka_pprof::{EngineOpts, FlameEngine};
    use object_store::{ObjectStore, memory::InMemory};

    /// The compacted object key spans the whole job -- the earliest start and
    /// the latest end across every input -- and names the level the output
    /// lands at. Two jobs over the same range with different inputs must not
    /// collide, which is what the fingerprint of the input keys is for.
    #[test]
    fn a_compacted_key_names_the_range_the_level_and_the_inputs() {
        let job = |inputs: &[&str], level: u32, min_ts, max_ts| CompactionJob {
            tenant: "tenant".to_string(),
            input_keys: inputs.iter().map(|key| (*key).to_string()).collect(),
            output_level: BlockLevel(level),
            min_ts,
            max_ts,
            row_count: 0,
        };
        let digest = format!(
            "{:016x}",
            input_key_fingerprint(&["a".to_string(), "b".to_string()])
        );

        check!(
            super::compacted_key(&job(&["a", "b"], 1, 100, 500))
                == format!("blocks/tenant/compacted/l1-100-500-{digest}.parquet")
        );
        check!(
            super::compacted_key(&job(&["a", "b"], 3, 100, 500))
                == format!("blocks/tenant/compacted/l3-100-500-{digest}.parquet"),
            "the level is part of the name"
        );
        check!(
            super::compacted_key(&job(&["a"], 1, 100, 500))
                != super::compacted_key(&job(&["a", "b"], 1, 100, 500)),
            "a different input list is a different key over the same range"
        );
    }

    use super::*;
    use crate::{
        blockbuilder::build_block,
        cold_store::ColdProfileStore,
        wal::{ProfileRecord, WalSample, WalSymbolSet},
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[tokio::test]
    async fn compact_blocks_rewrites_blocks_and_preserves_query_results() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", 5, "main");
        let rec_b = record("t", "api", 7, "worker");
        let meta_a = build_block(&store, "t", 0, std::slice::from_ref(&rec_a), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_b), (1, 1))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&rec_a, &rec_b] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index
                .add_series("t", labels.fingerprint(), &labels)
                .unwrap();
        }
        index.add_block(&meta_a);
        index.add_profile_block("t", &meta_a.object_key, vec![STACKTRACE_PARTITION]);
        index.add_block(&meta_b);
        index.add_profile_block("t", &meta_b.object_key, vec![STACKTRACE_PARTITION]);

        let meta = compact_blocks(
            &store,
            &mut index,
            "t",
            &[meta_a.object_key.clone(), meta_b.object_key.clone()],
            "blocks/t/compacted.parquet",
        )
        .await
        .unwrap();

        assert!(meta.row_count == 2);
        assert!(
            BlockIndex::candidate_blocks(&index, "t", 0, i64::MAX) == vec![meta.object_key.clone()]
        );
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        check!(fg.total == 12);
        for name in ["main", "worker"] {
            check!(fg.names.iter().any(|frame| frame == name));
        }
    }

    #[tokio::test]
    async fn a_reused_profile_input_key_cannot_overwrite_existing_compaction_objects() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old_record = record("t", "api", 5, "old");
        let other_record = record("t", "api", 7, "other");
        let old_input = build_block(&store, "t", 0, std::slice::from_ref(&old_record), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let other_input = build_block(&store, "t", 0, std::slice::from_ref(&other_record), (1, 1))
            .await
            .unwrap()
            .remove(0);
        let make_index = |first: &BlockMeta, first_record: &ProfileRecord| {
            let mut index = ProfileIndex::new();
            for record in [first_record, &other_record] {
                let labels = Labels::from_pairs(record.labels.iter().cloned());
                index
                    .add_series("t", labels.fingerprint(), &labels)
                    .unwrap();
            }
            for meta in [first, &other_input] {
                index.add_block(meta);
                index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
            }
            index
        };

        let mut old_index = make_index(&old_input, &old_record);
        let input_keys = vec![old_input.object_key.clone(), other_input.object_key.clone()];
        let old = compact_blocks(
            &store,
            &mut old_index,
            "t",
            &input_keys,
            "blocks/t/compacted.parquet",
        )
        .await
        .expect("the old inputs compact");
        let old_parquet = store
            .get(&Path::from(old.object_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let old_symdb_key = format!("{}.symdb", old.object_key);
        let old_symdb = store
            .get(&Path::from(old_symdb_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let replacement_record = record("t", "api", 11, "replacement");
        let replacement_input = build_block(
            &store,
            "t",
            0,
            std::slice::from_ref(&replacement_record),
            (0, 0),
        )
        .await
        .unwrap()
        .remove(0);
        check!(replacement_input.object_key == old_input.object_key);
        let mut replacement_index = make_index(&replacement_input, &replacement_record);
        let new = compact_blocks(
            &store,
            &mut replacement_index,
            "t",
            &input_keys,
            "blocks/t/compacted.parquet",
        )
        .await
        .expect("the replacement inputs compact");

        check!(old.object_key != new.object_key);
        check!(
            store
                .get(&Path::from(old.object_key))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                == old_parquet,
            "the old parquet object remains untouched"
        );
        check!(
            store
                .get(&Path::from(old_symdb_key))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                == old_symdb,
            "the old symbol database remains untouched"
        );
    }

    #[tokio::test]
    async fn compact_blocks_can_downsample_rows_into_time_buckets() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record_at("t", "api", 4, "main", 1_000);
        let rec_b = record_at("t", "api", 6, "main", 1_500);
        let rec_c = record_at("t", "api", 3, "worker", 3_000);
        let meta_a = build_block(&store, "t", 0, &[rec_a.clone(), rec_b.clone()], (0, 1))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_c), (2, 2))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&rec_a, &rec_b, &rec_c] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index
                .add_series("t", labels.fingerprint(), &labels)
                .unwrap();
        }
        index.add_block(&meta_a);
        index.add_profile_block("t", &meta_a.object_key, vec![STACKTRACE_PARTITION]);
        index.add_block(&meta_b);
        index.add_profile_block("t", &meta_b.object_key, vec![STACKTRACE_PARTITION]);

        let meta = compact_blocks_with_policy(
            &store,
            &mut index,
            "t",
            &[meta_a.object_key.clone(), meta_b.object_key.clone()],
            "blocks/t/downsampled.parquet",
            Some(DownsamplePolicy {
                resolution_ns: 1_000,
            }),
        )
        .await
        .unwrap();

        assert!(meta.row_count == 2);
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(fg.total == 13);
    }

    #[tokio::test]
    async fn recompacting_an_already_compacted_block_does_not_alias_partitions() {
        // Round-trip two compactions: build four fresh blocks, compact them
        // pairwise (so each compacted block has high-bit-based partitions), then
        // compact the two compacted blocks together. Without dense re-basing the
        // second compaction OR-folds the already-high partitions and aliases
        // them across blocks (and `copy_partition_from` rejects the non-empty
        // destination). Query results must be identical before and after.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", 5, "alpha");
        let rec_b = record("t", "api", 7, "bravo");
        let rec_c = record("t", "api", 11, "charlie");
        let rec_d = record("t", "api", 13, "delta");
        let mut index = ProfileIndex::new();

        let mut metas = Vec::new();
        for (idx, rec) in [&rec_a, &rec_b, &rec_c, &rec_d].into_iter().enumerate() {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index
                .add_series("t", labels.fingerprint(), &labels)
                .unwrap();
            let offset = i64::try_from(idx).unwrap();
            let bounds = (offset, offset);
            let meta = build_block(&store, "t", 0, std::slice::from_ref(rec), bounds)
                .await
                .unwrap()
                .remove(0);
            index.add_block(&meta);
            index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
            metas.push(meta);
        }

        // First compaction: a+b -> c1, c+d -> c2.
        let c1 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[metas[0].object_key.clone(), metas[1].object_key.clone()],
            "blocks/t/c1.parquet",
        )
        .await
        .unwrap();
        let c2 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[metas[2].object_key.clone(), metas[3].object_key.clone()],
            "blocks/t/c2.parquet",
        )
        .await
        .unwrap();

        // Query the once-compacted state for the baseline. `ProfileIndex` is
        // not `Clone`, so hand it to an `Arc`, run the query, then reclaim it
        // (the cold store / engine clones are dropped once the query resolves).
        let mut index = {
            let shared = Arc::new(index);
            let cold = Arc::new(ColdProfileStore::new(store.clone(), shared.clone()));
            let engine = FlameEngine::new(cold, EngineOpts::default());
            let before = engine
                .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
                .await
                .unwrap();
            assert!(before.total == 36);
            for name in ["alpha", "bravo", "charlie", "delta"] {
                assert!(
                    before.names.iter().any(|leaf| leaf == name),
                    "{name} missing"
                );
            }
            drop(engine);
            Arc::try_unwrap(shared).unwrap_or_else(|_| panic!("sole owner after query"))
        };
        let before_total = 36_i64;

        // Second compaction: c1 + c2 -> c3 (both inputs already compacted).
        let c3 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[c1.object_key.clone(), c2.object_key.clone()],
            "blocks/t/c3.parquet",
        )
        .await
        .unwrap();
        assert!(c3.row_count == 4);

        // After re-compaction every input partition must survive as a distinct
        // destination partition: four source partitions (two per input block)
        // must produce four distinct destinations with no aliasing.
        let final_partitions = index.stacktrace_partitions(&c3.object_key);
        assert!(final_partitions.len() == 4, "{final_partitions:?}");
        let distinct: BTreeSet<u64> = final_partitions.iter().copied().collect();
        assert!(
            distinct.len() == 4,
            "partitions aliased: {final_partitions:?}"
        );

        // Query results unchanged after the second compaction.
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let after = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        assert!(after.total == before_total);
        for name in ["alpha", "bravo", "charlie", "delta"] {
            assert!(after.names.iter().any(|leaf| leaf == name), "{name} lost");
        }
    }

    #[test]
    fn destination_partitions_rebases_high_bit_partitions_to_dense_local_ids() {
        // Already-compacted source partitions live in the high bits. Re-basing
        // them onto a fresh block base must produce dense, collision-free
        // destinations rather than OR-folding the high bits together.
        let sources = [1_u64 << 32, 2_u64 << 32, 3_u64 << 32];
        let map = destination_partitions(1, &sources).unwrap();
        let base = 2_u64 << 32;
        assert!(
            map == BTreeMap::from([
                (1_u64 << 32, base),
                (2_u64 << 32, base | 1),
                (3_u64 << 32, base | 2),
            ])
        );
        let dests: BTreeSet<u64> = map.values().copied().collect();
        assert!(dests.len() == 3);
    }

    /// A partition the map does not mention keeps its own id. Falling back to
    /// the default instead would collapse every unmapped partition onto zero,
    /// aliasing them together -- the exact failure
    /// [`recompacting_an_already_compacted_block_does_not_alias_partitions`]
    /// exists to prevent, but which no test reached through this function.
    #[test]
    fn remap_partitions_keeps_a_partition_the_map_does_not_mention() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            PCOL_STACKTRACE_PARTITION,
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![7_u64, 9, 7])) as ArrayRef],
        )
        .unwrap();

        let remapped =
            remap_partitions(&batch, &BTreeMap::from([(7_u64, 42_u64)])).expect("remaps");

        let column = remapped.column(0).as_primitive::<UInt64Type>();
        let values = (0..remapped.num_rows())
            .map(|row| column.value(row))
            .collect::<Vec<_>>();
        assert!(
            values == vec![42_u64, 9, 42],
            "9 is unmapped and keeps its id"
        );
    }

    fn meta(object_key: &str, min_ts: i64, max_ts: i64, row_count: usize) -> BlockMeta {
        BlockMeta {
            tenant: "t".to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            fingerprints: Vec::new(),
            level: BlockLevel::INGESTED,
        }
    }

    fn wide_policy(max_blocks_per_job: usize, max_level: u32) -> CompactionPolicy {
        CompactionPolicy::new(
            max_blocks_per_job,
            usize::MAX,
            BlockLevel(max_level),
            i64::MAX,
        )
    }

    #[test]
    fn plan_compactions_groups_blocks_by_tenant_in_time_order() {
        let mut index = ProfileIndex::new();
        index.replace_profile_blocks(
            "t",
            &[],
            &[
                (meta("b.parquet", 10, 20, 1), vec![0]),
                (meta("a.parquet", 0, 5, 1), vec![0]),
            ],
        );

        let jobs = plan_compactions(&index, wide_policy(2, 4));

        assert!(jobs.len() == 1);
        assert!(jobs[0].input_keys == vec!["a.parquet".to_string(), "b.parquet".to_string()]);
        assert!(jobs[0].output_level == BlockLevel(1));
    }

    /// The planner has to stop. Each pass replaces at least two blocks with
    /// one a level higher, and a block at the top of the ladder is never an
    /// input again, so a compactor left running against a quiet index plans
    /// nothing after a bounded number of passes.
    #[tokio::test]
    async fn repeated_passes_climb_the_ladder_and_then_plan_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        let mut records = Vec::new();
        for n in 0..4_i64 {
            let rec = record_at("t", "api", 1, "main", 1_000 + n);
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index
                .add_series("t", labels.fingerprint(), &labels)
                .unwrap();
            let block = build_block(&store, "t", 0, std::slice::from_ref(&rec), (n, n))
                .await
                .unwrap()
                .remove(0);
            index.add_block(&block);
            index.add_profile_block("t", &block.object_key, vec![STACKTRACE_PARTITION]);
            records.push(rec);
        }

        let policy = wide_policy(2, 2);
        let mut levels = Vec::new();
        let mut passes = 0;
        loop {
            let metas = compact_once(&store, &mut index, policy).await.unwrap();
            if metas.is_empty() {
                break;
            }
            passes += 1;
            assert!(passes <= 4, "compaction did not converge");
            levels.push(
                metas
                    .iter()
                    .map(|meta| index.block_level(&meta.object_key))
                    .collect::<Vec<_>>(),
            );
        }

        // Four blocks pair into two at level 1, then into one at level 2,
        // which is the cap: the third pass plans nothing.
        check!(levels == vec![vec![BlockLevel(1); 2], vec![BlockLevel(2)]]);
        check!(passes == 2);
        check!(BlockIndex::block_count(&index, "t") == 1);

        // The surviving block still answers the query the four inputs did.
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        check!(fg.total == 4);
    }

    fn record(tenant: &str, service: &str, value: i64, function: &str) -> ProfileRecord {
        record_at(tenant, service, value, function, 1000)
    }

    fn record_at(
        tenant: &str,
        service: &str,
        value: i64,
        function: &str,
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
                stacktrace_location_refs: vec![0],
                value,
                timestamp_ns,
                span_id: None,
                trace_id: None,
            }],
            symbols: symbols(function),
        }
    }

    fn symbols(function: &str) -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), function.to_string()],
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

mod compact_blocks;
mod compact_blocks_with_policy;
mod compact_once;
mod compact_once_with_policy;
mod compacted_key;
mod destination_partitions;
mod downsample_batches;
mod downsample_key;
mod downsample_policy;
mod load_symdb;
mod plan_compactions;
mod remap_partitions;
mod sample_group_buffer;
mod source_partitions;

pub use compact_blocks::compact_blocks;
pub use compact_blocks_with_policy::compact_blocks_with_policy;
pub use compact_once::compact_once;
pub use compact_once_with_policy::compact_once_with_policy;
use compacted_key::compacted_key;
use destination_partitions::destination_partitions;
use downsample_batches::downsample_batches;
use downsample_key::DownsampleKey;
pub use downsample_policy::DownsamplePolicy;
use load_symdb::load_symdb;
pub use plan_compactions::plan_compactions;
use remap_partitions::remap_partitions;
use sample_group_buffer::SampleGroupBuffer;
use source_partitions::source_partitions;
