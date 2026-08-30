//! Profile block compaction.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryArray, UInt64Array},
    datatypes::{Int32Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, ProfileIndex,
    ProfileSampleRow, encode_profile_samples,
};
use krabka_pprof::SymbolDb;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use crate::{blockbuilder::STACKTRACE_PARTITION, error::ProfilesError};

#[cfg(test)]
mod tests {

    /// `fnv1a` hashes a list of keys into one value, folding a separator
    /// between them so that where one key ends and the next begins is part of
    /// the input. Two compaction inputs that join differently must not share
    /// an output name.
    ///
    /// The expected hashes are stated outright rather than compared against
    /// each other. Inequality is too weak a claim for a hash: dropping the
    /// separator, or leaving it unmixed, or replacing the xor with an or, all
    /// still produce different values for different inputs, and all survived
    /// a version of this test that only asserted the values differed.
    #[test]
    fn hashing_keys_folds_a_separator_between_them() {
        let hash = |keys: &[&str]| {
            super::fnv1a(&keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>())
        };

        // No keys leaves the offset basis untouched.
        check!(hash(&[]) == 0xcbf2_9ce4_8422_2325);

        // One empty key still folds a separator, so it is not the same as no
        // key at all.
        check!(hash(&[""]) == 0xaf64_724c_8602_eb6e);
        check!(hash(&["a"]) == 0x089b_c907_b544_c769);

        // Order is part of the input.
        check!(hash(&["a", "b"]) == 0xd2b3_7181_9297_f98a);
        check!(hash(&["b", "a"]) == 0x0185_7199_9fe5_8c66);

        // So is where the keys divide: the same bytes split two ways, and
        // joined into one, give three different hashes.
        check!(hash(&["ab", "c"]) == 0x20ba_9b30_25a8_b421);
        check!(hash(&["a", "bc"]) == 0xa0a3_542c_19b9_00ab);
        check!(hash(&["abc"]) == 0xfc18_2483_ee08_06dc);
    }
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_blockstore::{BlockIndex, Labels};
    use krabka_pprof::{EngineOpts, FlameEngine};
    use object_store::{ObjectStore, memory::InMemory};

    /// The compactor hashes a *list* of keys, not a blob, so it folds a 0xff
    /// separator in after each one. Without it a single key `ab` and the pair
    /// `a`, `b` would collide, and two different compaction inputs would
    /// share a key.
    ///
    /// The expected values are computed from the FNV-1a definition rather
    /// than captured from this implementation.
    #[test]
    fn compaction_keys_hash_their_boundaries_not_just_their_bytes() {
        let hash = |keys: &[&str]| {
            super::fnv1a(&keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>())
        };

        check!(
            hash(&[]) == 0xcbf2_9ce4_8422_2325,
            "no keys is the offset basis"
        );
        check!(hash(&["a"]) == 0x089b_c907_b544_c769);
        check!(hash(&["ab"]) == 0xe720_2e19_0542_452f);
        check!(hash(&["a", "b"]) == 0xd2b3_7181_9297_f98a);

        // The three relationships the separator exists to guarantee.
        check!(
            hash(&["ab"]) != hash(&["a", "b"]),
            "a split is not the same as a join"
        );
        check!(hash(&["a", "b"]) != hash(&["b", "a"]), "order matters");
        check!(hash(&["a"]) != hash(&[]), "a key is not nothing");
    }

    /// The compacted object key spans the whole input: the earliest start and
    /// the latest end across every block, not the first block's range. The
    /// blocks below are deliberately out of order so a key built from
    /// position rather than from the extremes is visibly wrong.
    #[test]
    fn a_compacted_key_spans_the_whole_input_range() {
        let block = |min_ts, max_ts| BlockMeta {
            tenant: "t".to_string(),
            object_key: String::new(),
            min_ts,
            max_ts,
            row_count: 0,
            fingerprints: vec![],
        };
        let inputs = vec!["a".to_string(), "b".to_string()];
        let digest = format!("{:016x}", super::fnv1a(&inputs));

        let blocks = [block(300, 400), block(100, 500), block(200, 250)];
        check!(
            super::compacted_key("tenant", &blocks, &inputs)
                == format!("blocks/tenant/compacted/100-500-{digest}.parquet")
        );

        // A single block spans itself.
        check!(
            super::compacted_key("tenant", &[block(7, 9)], &inputs)
                == format!("blocks/tenant/compacted/7-9-{digest}.parquet")
        );

        // No blocks leaves both ends at zero rather than failing.
        check!(
            super::compacted_key("tenant", &[], &inputs)
                == format!("blocks/tenant/compacted/0-0-{digest}.parquet")
        );

        // The digest covers the inputs, so a different input list is a
        // different key even over the same range.
        check!(
            super::compacted_key("tenant", &blocks, &["a".to_string()])
                != super::compacted_key("tenant", &blocks, &inputs)
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
            index.add_series("t", labels.fingerprint(), &labels);
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
            index.add_series("t", labels.fingerprint(), &labels);
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
            index.add_series("t", labels.fingerprint(), &labels);
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

    #[test]
    fn plan_compactions_groups_blocks_by_tenant_in_time_order() {
        let mut index = ProfileIndex::new();
        index.replace_profile_blocks(
            "t",
            &[],
            &[
                (
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b.parquet".to_string(),
                        min_ts: 10,
                        max_ts: 20,
                        row_count: 1,
                        fingerprints: Vec::new(),
                    },
                    vec![0],
                ),
                (
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "a.parquet".to_string(),
                        min_ts: 0,
                        max_ts: 5,
                        row_count: 1,
                        fingerprints: Vec::new(),
                    },
                    vec![0],
                ),
            ],
        );

        let jobs = plan_compactions(&index, 2);

        assert!(jobs.len() == 1);
        assert!(jobs[0].input_keys == vec!["a.parquet".to_string(), "b.parquet".to_string()]);
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

// === split-modules: generated submodules ===
mod collect_meta;
mod compact_blocks;
mod compact_blocks_with_policy;
mod compact_once;
mod compact_once_with_policy;
mod compacted_key;
mod compaction_job;
mod destination_partitions;
mod downsample_batches;
mod downsample_key;
mod downsample_policy;
mod fnv1a;
mod load_batches;
mod load_symdb;
mod plan_compactions;
mod remap_partitions;
mod source_partitions;
mod write_batches;

use collect_meta::collect_meta;
pub use compact_blocks::compact_blocks;
pub use compact_blocks_with_policy::compact_blocks_with_policy;
pub use compact_once::compact_once;
pub use compact_once_with_policy::compact_once_with_policy;
use compacted_key::compacted_key;
pub use compaction_job::CompactionJob;
use destination_partitions::destination_partitions;
use downsample_batches::downsample_batches;
use downsample_key::DownsampleKey;
pub use downsample_policy::DownsamplePolicy;
use fnv1a::fnv1a;
use load_batches::load_batches;
use load_symdb::load_symdb;
pub use plan_compactions::plan_compactions;
use remap_partitions::remap_partitions;
use source_partitions::source_partitions;
use write_batches::write_batches;
