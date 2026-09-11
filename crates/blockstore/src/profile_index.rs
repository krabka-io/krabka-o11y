//! Profiles block index.
//!
//! `ProfileIndex` embeds [`Index`] for label postings and matcher resolution.
//! It then adds the profile-type lookup and the stacktrace-partition map that
//! Pyroscope-compatible profiles queries need.

use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use krabka_units::prelude::*;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    compaction::{BlockLevel, CompactionCandidate, level_above},
    error::{BlockStoreError, Result},
    index::{ByteReader, Index, IndexShardRange, decode_index_shard, push_uvarint},
    index_snapshot::{
        DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, PendingBlockAdditions,
        PendingBlockRemovals, PendingRemoval, SnapshotManifest, UNBOUNDED_SHARD_RANGE,
        put_manifest_snapshot, put_shard_payload, read_latest_snapshot_manifest,
        read_shard_payload, shard_payload_content_hash, shard_payload_object_key,
        shard_ranges_for_span,
    },
    labels::{Labels, SeriesFingerprint},
    matcher::LabelMatcher,
};

#[cfg(test)]
mod tests {
    use assert2::check;
    use object_store::{ObjectStoreExt as _, path::Path};

    use super::*;
    use crate::{
        error::BlockStoreError,
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    const CPU_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    const HEAP_TYPE: &str = "memory:alloc_space:bytes:space:bytes";

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::from_pairs(pairs.iter().copied())
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn profile_labels(name: &str, profile_type: &str, service_name: &str) -> Labels {
        labels(&[
            ("__name__", name),
            ("__profile_type__", profile_type),
            ("service_name", service_name),
        ])
    }

    fn block_keys(index: &ProfileIndex) -> Vec<String> {
        let mut keys: Vec<String> = index
            .all_blocks()
            .into_iter()
            .map(|meta| meta.object_key)
            .collect();
        keys.sort();
        keys
    }

    fn seed() -> ProfileIndex {
        let mut index = ProfileIndex::new();
        let cpu = labels(&[
            ("__name__", "process_cpu"),
            ("__profile_type__", CPU_TYPE),
            ("service_name", "checkout"),
        ]);
        let heap = labels(&[
            ("__name__", "memory"),
            ("__profile_type__", HEAP_TYPE),
            ("service_name", "checkout"),
        ]);
        index.add_series("t", cpu.fingerprint(), &cpu).unwrap();
        index.add_series("t", heap.fingerprint(), &heap).unwrap();
        index
    }

    /// Registers one block carrying `seed`'s series, with stacktrace
    /// partitions of its own.
    ///
    /// The partitions live beside the block record in the shard the block
    /// falls in, so a block has to exist for them to be published at all. The
    /// production flush registers the block and then its partitions, in that
    /// order, which is what this reproduces.
    fn seed_partitioned_block(index: &mut ProfileIndex) {
        let fingerprints = index
            .matching_fingerprints("t", &[])
            .expect("an empty matcher set selects every series")
            .into_iter()
            .collect::<Vec<_>>();
        <ProfileIndex as BlockIndex>::add_block(
            index,
            &BlockMeta {
                tenant: "t".to_string(),
                object_key: "blocks/p1.parquet".to_string(),
                min_ts: 0,
                max_ts: 100,
                row_count: 3,
                fingerprints,
                level: BlockLevel::INGESTED,
            },
        );
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1]);
    }

    fn seed_with_blocks() -> (
        ProfileIndex,
        SeriesFingerprint,
        SeriesFingerprint,
        SeriesFingerprint,
    ) {
        let mut index = ProfileIndex::new();
        let cpu_checkout = profile_labels("process_cpu", CPU_TYPE, "checkout");
        let heap_checkout = profile_labels("memory", HEAP_TYPE, "checkout");
        let cpu_payments = profile_labels("process_cpu", CPU_TYPE, "payments");
        let cpu_checkout_fp = cpu_checkout.fingerprint();
        let heap_checkout_fp = heap_checkout.fingerprint();
        let cpu_payments_fp = cpu_payments.fingerprint();

        index
            .add_series("t", cpu_checkout_fp, &cpu_checkout)
            .unwrap();
        index
            .add_series("t", heap_checkout_fp, &heap_checkout)
            .unwrap();
        index
            .add_series("t", cpu_payments_fp, &cpu_payments)
            .unwrap();

        for meta in [
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-checkout.parquet".to_string(),
                min_ts: 100,
                max_ts: 199,
                row_count: 10,
                fingerprints: vec![cpu_checkout_fp],
                level: BlockLevel::INGESTED,
            },
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "heap-checkout.parquet".to_string(),
                min_ts: 300,
                max_ts: 399,
                row_count: 20,
                fingerprints: vec![heap_checkout_fp],
                level: BlockLevel::INGESTED,
            },
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-payments.parquet".to_string(),
                min_ts: 150,
                max_ts: 250,
                row_count: 30,
                fingerprints: vec![cpu_payments_fp],
                level: BlockLevel::INGESTED,
            },
        ] {
            <ProfileIndex as BlockIndex>::add_block(&mut index, &meta);
        }

        (index, cpu_checkout_fp, heap_checkout_fp, cpu_payments_fp)
    }

    #[test]
    fn snapshot_size_cap_is_256_mib() {
        assert2::assert!(MAX_PROFILE_INDEX_SNAPSHOT_BYTES == mebibytes(256));
        assert2::assert!(MAX_PROFILE_INDEX_SNAPSHOT_BYTES.bytes_u64() == 256 * 1024 * 1024);
    }

    #[test]
    fn profile_types_lists_distinct_type_strings() {
        let index = seed();
        let mut types = index.profile_types("t");
        types.sort();
        assert2::assert!(types == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(index.profile_types("nope").is_empty());
    }

    #[test]
    fn profile_type_index_maps_type_to_its_series() {
        let index = seed();
        let cpu_fps = index.fingerprints_for_profile_type("t", CPU_TYPE);
        let heap_fps = index.fingerprints_for_profile_type("t", HEAP_TYPE);
        assert2::assert!(
            cpu_fps
                == BTreeSet::from([
                    profile_labels("process_cpu", CPU_TYPE, "checkout").fingerprint()
                ])
        );
        assert2::assert!(
            heap_fps
                == BTreeSet::from([profile_labels("memory", HEAP_TYPE, "checkout").fingerprint()])
        );
    }

    #[test]
    fn profile_index_matching_and_block_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, cpu_payments) = seed_with_blocks();

        assert2::assert!(
            index
                .matching_fingerprints(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::from([cpu_checkout, heap_checkout])
        );
        assert2::assert!(
            index
                .select_fingerprints(
                    "t",
                    CPU_TYPE,
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::from([cpu_checkout])
        );
        assert2::assert!(
            index.select_fingerprints("t", CPU_TYPE, &[]).unwrap()
                == BTreeSet::from([cpu_checkout, cpu_payments])
        );
        assert2::assert!(
            index.candidate_blocks_for_series(
                "t",
                &BTreeSet::from([cpu_checkout, cpu_payments]),
                175,
                180,
            ) == strings(&["cpu-checkout.parquet", "cpu-payments.parquet"])
        );
        assert2::assert!(index.block_time_bounds("t", 175, 180) == Some((100, 250)));
        assert2::assert!(index.block_time_bounds("t", 450, 500) == None);
        assert2::assert!(
            BlockIndex::candidate_blocks(&index, "t", 175, 180)
                == strings(&["cpu-checkout.parquet", "cpu-payments.parquet"])
        );
        assert2::assert!(BlockIndex::block_count(&index, "t") == 3);
    }

    #[test]
    fn profile_index_profile_type_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, _) = seed_with_blocks();

        assert2::assert!(index.profile_types_for_time("t", 175, 180) == strings(&[CPU_TYPE]));
        assert2::assert!(index.profile_types_for_time("t", 320, 330) == strings(&[HEAP_TYPE]));
        assert2::assert!(
            index.profile_types_for_fingerprints("t", &BTreeSet::from([cpu_checkout]))
                == strings(&[CPU_TYPE])
        );
        assert2::assert!(
            index.profile_types_for_fingerprints("t", &BTreeSet::from([heap_checkout]))
                == strings(&[HEAP_TYPE])
        );
    }

    #[test]
    fn profile_index_label_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, _) = seed_with_blocks();

        assert2::assert!(
            index
                .label_values_for_time("t", "service_name", &[], 175, 180)
                .unwrap()
                == strings(&["checkout", "payments"])
        );
        assert2::assert!(
            index.label_values_for_fingerprints(
                "t",
                "service_name",
                &BTreeSet::from([heap_checkout])
            ) == strings(&["checkout"])
        );
        assert2::assert!(
            index.label_names_for_time("t", &[], 175, 180).unwrap()
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_names_for_fingerprints("t", &BTreeSet::from([cpu_checkout]))
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_names("t") == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_values("t", "service_name") == strings(&["checkout", "payments"])
        );
        assert2::assert!(
            index
                .label_names_for(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index
                .label_values_for(
                    "t",
                    "__name__",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == strings(&["memory", "process_cpu"])
        );
    }

    #[test]
    fn profile_index_series_helpers_return_projected_metadata() {
        let (index, cpu_checkout, heap_checkout, cpu_payments) = seed_with_blocks();

        let mut blocks = index.all_blocks();
        blocks.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        assert2::assert!(
            index
                .series_for_time("t", &[], &["service_name".to_string()], 175, 180)
                .unwrap()
                == vec![
                    vec![("service_name".to_string(), "checkout".to_string())],
                    vec![("service_name".to_string(), "payments".to_string())],
                ]
        );
        assert2::assert!(
            index.series_for_fingerprints("t", &BTreeSet::from([heap_checkout]), &[])
                == vec![vec![
                    ("__name__".to_string(), "memory".to_string()),
                    ("__profile_type__".to_string(), HEAP_TYPE.to_string()),
                    ("service_name".to_string(), "checkout".to_string()),
                ]]
        );
        assert2::assert!(
            index
                .series(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")],
                    &["__name__".to_string()],
                )
                .unwrap()
                == vec![
                    vec![("__name__".to_string(), "memory".to_string())],
                    vec![("__name__".to_string(), "process_cpu".to_string())],
                ]
        );
        assert2::assert!(
            blocks
                == vec![
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "cpu-checkout.parquet".to_string(),
                        min_ts: 100,
                        max_ts: 199,
                        row_count: 10,
                        fingerprints: vec![cpu_checkout],
                        level: BlockLevel::INGESTED,
                    },
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "cpu-payments.parquet".to_string(),
                        min_ts: 150,
                        max_ts: 250,
                        row_count: 30,
                        fingerprints: vec![cpu_payments],
                        level: BlockLevel::INGESTED,
                    },
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "heap-checkout.parquet".to_string(),
                        min_ts: 300,
                        max_ts: 399,
                        row_count: 20,
                        fingerprints: vec![heap_checkout],
                        level: BlockLevel::INGESTED,
                    },
                ]
        );
    }

    #[test]
    fn resolve_reuses_series_postings() {
        let index = seed();
        let got = index
            .resolve(
                "t",
                &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")],
            )
            .unwrap();
        assert2::assert!(
            got == BTreeSet::from([
                profile_labels("process_cpu", CPU_TYPE, "checkout").fingerprint(),
                profile_labels("memory", HEAP_TYPE, "checkout").fingerprint(),
            ])
        );
    }

    #[test]
    fn stacktrace_partition_map_records_block_partitions() {
        let mut index = seed();
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1, 2]);
        assert2::assert!(index.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1, 2]);
        assert2::assert!(
            index
                .stacktrace_partitions("blocks/absent.parquet")
                .is_empty()
        );
    }

    #[test]
    fn replace_profile_blocks_removes_old_partition_maps() {
        let mut index = seed();
        let labels = labels(&[
            ("__name__", "process_cpu"),
            (
                "__profile_type__",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            ),
            ("service_name", "checkout"),
        ]);
        let fp = labels.fingerprint();
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "old.parquet".to_string(),
            min_ts: 0,
            max_ts: 10,
            row_count: 1,
            fingerprints: vec![fp],
            level: BlockLevel::INGESTED,
        });
        index.add_profile_block("t", "old.parquet", vec![0]);

        index.replace_profile_blocks(
            "t",
            &["old.parquet".to_string()],
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "new.parquet".to_string(),
                    min_ts: 0,
                    max_ts: 10,
                    row_count: 1,
                    fingerprints: vec![fp],
                    level: BlockLevel::INGESTED,
                },
                vec![99],
            )],
        );

        assert2::assert!(index.stacktrace_partitions("old.parquet").is_empty());
        assert2::assert!(index.stacktrace_partitions("new.parquet") == vec![99]);
        assert2::assert!(
            BlockIndex::candidate_blocks(&index, "t", 0, 10) == vec!["new.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::memory::InMemory;

        let mut index = seed();
        seed_partitioned_block(&mut index);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        assert2::assert!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }

    #[tokio::test]
    async fn missing_latest_snapshot_is_empty_but_corruption_is_an_error() {
        use object_store::{PutPayload, memory::InMemory};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let empty = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            &store,
            "index/profiles.json",
            crate::DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await
        .unwrap();
        assert2::assert!(empty.profile_types("tenant-a").is_empty());

        // The object a generation swaps is the manifest, so that is what a
        // reader has to refuse to guess at.
        store
            .put(
                &Path::from(format!(
                    "{}/00000000000000000000.json",
                    crate::index_snapshot_prefix_for_key("index/profiles.json")
                )),
                PutPayload::from(b"not-json".to_vec()),
            )
            .await
            .unwrap();
        let corrupted = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            &store,
            "index/profiles.json",
            crate::DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await;
        assert2::assert!(matches!(corrupted, Err(BlockStoreError::Serde(_))));
    }

    #[tokio::test]
    async fn latest_snapshot_round_trips_without_writing_the_key_itself() {
        use object_store::memory::InMemory;

        let mut index = seed();
        seed_partitioned_block(&mut index);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let snapshot_key = index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        check!(snapshot_key.starts_with("index/profiles/snapshots/"));
        check!(
            store
                .head(&Path::from("index/profiles.json"))
                .await
                .is_err()
        );
        assert2::assert!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }

    #[tokio::test]
    async fn latest_snapshot_retains_bounded_snapshot_set() {
        use futures::StreamExt as _;
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        for _ in 0..(crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_RETAIN + 3) {
            index
                .save_latest_snapshot(&store, "index/profiles.json")
                .await
                .unwrap();
        }

        let prefix = Path::from(crate::index_snapshot_prefix_for_key("index/profiles.json"));
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(meta) = stream.next().await {
            meta.unwrap();
            count += 1;
        }

        assert2::assert!(count == crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_RETAIN);
    }

    #[tokio::test]
    async fn configurable_snapshot_policy_caps_loads_and_retention() {
        use futures::StreamExt as _;
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let retain = crate::IndexSnapshotRetain::new(2).unwrap();

        for _ in 0..4 {
            index
                .save_latest_snapshot_with_retain(&store, "index/profiles.json", retain)
                .await
                .unwrap();
        }

        let prefix = Path::from(crate::index_snapshot_prefix_for_key("index/profiles.json"));
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(meta) = stream.next().await {
            meta.unwrap();
            count += 1;
        }
        assert_eq!(count, 2);

        let cap = krabka_units::bytes(1);
        let got =
            ProfileIndex::load_latest_snapshot_with_max_bytes(&store, "index/profiles.json", cap)
                .await;
        assert2::assert!(matches!(got, Err(BlockStoreError::InvalidBlock(_))));
    }

    /// A profiles block builder publishes its own blocks without erasing
    /// anyone else's, even when it starts from an empty index.
    #[tokio::test]
    async fn snapshot_writes_merge_into_the_newest_generation() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (published, ..) = seed_with_blocks();
        published
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let mut fresh = ProfileIndex::new();
        let shipping = profile_labels("process_cpu", CPU_TYPE, "shipping");
        let shipping_fp = shipping.fingerprint();
        fresh.add_series("t", shipping_fp, &shipping).unwrap();
        <ProfileIndex as BlockIndex>::add_block(
            &mut fresh,
            &BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-shipping.parquet".to_string(),
                min_ts: 100,
                max_ts: 199,
                row_count: 5,
                fingerprints: vec![shipping_fp],
                level: BlockLevel::INGESTED,
            },
        );
        fresh.add_profile_block("t", "cpu-shipping.parquet", vec![7]);
        fresh
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        check!(
            block_keys(&loaded)
                == strings(&[
                    "cpu-checkout.parquet",
                    "cpu-payments.parquet",
                    "cpu-shipping.parquet",
                    "heap-checkout.parquet",
                ])
        );
        check!(loaded.stacktrace_partitions("cpu-shipping.parquet") == vec![7]);
        check!(
            loaded.label_values("t", "service_name")
                == strings(&["checkout", "payments", "shipping"])
        );
        check!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
    }

    /// The merge is a union, so a compaction swap has to be replayed against
    /// the merge base. Otherwise every snapshot write would resurrect the
    /// blocks the compactor just replaced.
    #[tokio::test]
    async fn compaction_removals_are_not_resurrected_by_the_merge() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (mut index, cpu_checkout_fp, heap_checkout_fp, _) = seed_with_blocks();
        index.add_profile_block("t", "cpu-checkout.parquet", vec![1]);
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        index.replace_profile_blocks(
            "t",
            &strings(&["cpu-checkout.parquet", "heap-checkout.parquet"]),
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "compacted.parquet".to_string(),
                    min_ts: 100,
                    max_ts: 399,
                    row_count: 30,
                    fingerprints: vec![cpu_checkout_fp, heap_checkout_fp],
                    level: BlockLevel::INGESTED,
                },
                vec![9],
            )],
        );
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        check!(block_keys(&loaded) == strings(&["compacted.parquet", "cpu-payments.parquet"]));
        check!(
            loaded
                .stacktrace_partitions("cpu-checkout.parquet")
                .is_empty()
        );
        check!(loaded.stacktrace_partitions("compacted.parquet") == vec![9]);
    }

    /// The race the trace index has too, and the one a compactor running every
    /// few minutes makes routine.
    ///
    /// A writer that read a block from a snapshot and has held it in memory
    /// since has no removal to replay when a *concurrent* compactor retires
    /// that block. A merge that contributed every block the writer names would
    /// put the compaction's input back beside its output, and every sample in
    /// it would be read twice until the writer restarted. Only blocks
    /// registered since the writer's last successful write are contributed, so
    /// the swap survives.
    #[tokio::test]
    async fn a_stale_writer_does_not_resurrect_the_block_a_compactor_replaced() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (mut writer, cpu_checkout_fp, ..) = seed_with_blocks();
        writer.add_profile_block("t", "cpu-checkout.parquet", vec![1]);
        writer
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        // Another process loads that snapshot and swaps the block out.
        let mut compactor = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        compactor.replace_profile_blocks(
            "t",
            &strings(&["cpu-checkout.parquet"]),
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "compacted.parquet".to_string(),
                    min_ts: 100,
                    max_ts: 199,
                    row_count: 10,
                    fingerprints: vec![cpu_checkout_fp],
                    level: BlockLevel::INGESTED,
                },
                vec![9],
            )],
        );
        compactor
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        // The first writer still names the replaced block, and writes another.
        let shipping = profile_labels("process_cpu", CPU_TYPE, "shipping");
        let shipping_fp = shipping.fingerprint();
        writer.add_series("t", shipping_fp, &shipping).unwrap();
        <ProfileIndex as BlockIndex>::add_block(
            &mut writer,
            &BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-shipping.parquet".to_string(),
                min_ts: 500,
                max_ts: 599,
                row_count: 5,
                fingerprints: vec![shipping_fp],
                level: BlockLevel::INGESTED,
            },
        );
        writer.add_profile_block("t", "cpu-shipping.parquet", vec![7]);
        writer
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        check!(
            block_keys(&loaded)
                == strings(&[
                    "compacted.parquet",
                    "cpu-payments.parquet",
                    "cpu-shipping.parquet",
                    "heap-checkout.parquet",
                ])
        );
        check!(
            loaded
                .stacktrace_partitions("cpu-checkout.parquet")
                .is_empty()
        );
        check!(loaded.stacktrace_partitions("cpu-shipping.parquet") == vec![7]);
    }

    /// The other direction, and the one that rules out publishing a stale
    /// compaction output.
    ///
    /// Object keys are derived from what a block holds, not minted, so the same
    /// key can be handed out again for a different set of inputs. A removal
    /// recorded by name alone would drop the block written under that key
    /// since, and every later snapshot would carry the drop forward: a live
    /// object nothing names any more. Publishing both it and the stale
    /// compaction output would double-count the retired samples, so the whole
    /// replacement must instead be rejected.
    #[tokio::test]
    async fn a_reused_input_key_rejects_the_stale_profile_compaction() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (mut published, cpu_checkout_fp, ..) = seed_with_blocks();
        published.add_profile_block("t", "cpu-checkout.parquet", vec![1]);
        published
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        // The compactor retires the block, but has not published yet.
        let mut compactor = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        compactor.replace_profile_blocks(
            "t",
            &strings(&["cpu-checkout.parquet"]),
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "compacted.parquet".to_string(),
                    min_ts: 100,
                    max_ts: 199,
                    row_count: 10,
                    fingerprints: vec![cpu_checkout_fp],
                    level: BlockLevel::INGESTED,
                },
                vec![9],
            )],
        );

        // A third writer mints the same key for a different block and gets
        // there first.
        let mut reuser = ProfileIndex::new();
        let shipping = profile_labels("process_cpu", CPU_TYPE, "shipping");
        let shipping_fp = shipping.fingerprint();
        reuser.add_series("t", shipping_fp, &shipping).unwrap();
        <ProfileIndex as BlockIndex>::add_block(
            &mut reuser,
            &BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-checkout.parquet".to_string(),
                min_ts: 900,
                max_ts: 999,
                row_count: 3,
                fingerprints: vec![shipping_fp],
                level: BlockLevel::INGESTED,
            },
        );
        reuser.add_profile_block("t", "cpu-checkout.parquet", vec![5]);
        reuser
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let result = compactor
            .save_latest_snapshot(&store, "index/profiles.json")
            .await;

        assert2::assert!(matches!(
            result,
            Err(BlockStoreError::InvalidBlock(message))
                if message.contains("cpu-checkout.parquet")
        ));

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        check!(
            block_keys(&loaded)
                == strings(&[
                    "cpu-checkout.parquet",
                    "cpu-payments.parquet",
                    "heap-checkout.parquet",
                ])
        );
        // The live record under the reused key is the new one.
        check!(loaded.stacktrace_partitions("cpu-checkout.parquet") == vec![5]);
        check!(
            loaded.candidate_blocks_for_series("t", &BTreeSet::from([shipping_fp]), 900, 999)
                == strings(&["cpu-checkout.parquet"])
        );
    }

    /// One day, in the milliseconds the profiles index counts.
    const PROFILE_DAY_MS: i64 = 24 * 60 * 60 * 1_000;

    /// Registers one block a day for `days`, each carrying its own series.
    fn seed_days(index: &mut ProfileIndex, days: i64) {
        for day in 0..days {
            let labels = profile_labels("process_cpu", CPU_TYPE, &format!("service-{day}"));
            let fingerprint = labels.fingerprint();
            index.add_series("t", fingerprint, &labels).unwrap();
            <ProfileIndex as BlockIndex>::add_block(
                index,
                &BlockMeta {
                    tenant: "t".to_string(),
                    object_key: format!("blocks/day-{day}.parquet"),
                    min_ts: day * PROFILE_DAY_MS,
                    max_ts: day * PROFILE_DAY_MS + PROFILE_DAY_MS - 1,
                    row_count: 100,
                    fingerprints: vec![fingerprint],
                    level: BlockLevel::INGESTED,
                },
            );
            index.add_profile_block(
                "t",
                &format!("blocks/day-{day}.parquet"),
                vec![day.cast_unsigned()],
            );
        }
    }

    async fn payload_object_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        use futures::StreamExt as _;

        let mut listing = store.list(Some(&Path::from("index/profiles/payloads")));
        let mut keys = Vec::new();
        while let Some(meta) = listing.next().await {
            keys.push(meta.unwrap().location.to_string());
        }
        keys.sort();
        keys
    }

    /// A flush publishes the shards its own blocks fall in, and names the rest
    /// by the keys the previous generation gave them.
    ///
    /// Payloads are immutable and content-addressed, so a shard that changed
    /// is a new object and a shard that did not is the object that was already
    /// there. Counting the objects therefore counts the shards a flush
    /// rewrote.
    #[tokio::test]
    async fn a_flush_publishes_only_the_shards_its_own_blocks_fall_in() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        seed_days(&mut index, 30);
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        let before = payload_object_keys(&store).await;
        check!(before.len() == 30, "one shard a day");

        seed_days(&mut index, 31);
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let after = payload_object_keys(&store).await;
        // Exactly one new object: the thirty-first day. The thirty shards that
        // did not change were carried into the new manifest by key.
        check!(after.len() == before.len() + 1);
        for key in &before {
            check!(after.contains(key), "{key} was republished");
        }
    }

    /// A reader that knows its time range fetches the payloads that meet it and
    /// no others, and what it gets back is the same records the whole-index
    /// load has for that range.
    #[tokio::test]
    async fn a_query_about_one_day_loads_one_days_shard() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        seed_days(&mut index, 30);
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let day = 7;
        let scoped = ProfileIndex::load_latest_snapshot_for_range_with_max_bytes(
            &store,
            "index/profiles.json",
            "t",
            day * PROFILE_DAY_MS,
            day * PROFILE_DAY_MS + PROFILE_DAY_MS - 1,
            crate::DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await
        .unwrap();

        check!(block_keys(&scoped) == strings(&["blocks/day-7.parquet"]));
        check!(scoped.stacktrace_partitions("blocks/day-7.parquet") == vec![7]);
        // The series of the day it read, and none of the other twenty-nine.
        check!(
            scoped.label_values("t", "service_name") == strings(&["service-7"]),
            "a scoped load holds the range it asked about"
        );
    }

    /// Profile types are a function of the `__profile_type__` label, so they
    /// are replayed on load rather than published. A load that did not replay
    /// them would answer `/label-values` with nothing.
    #[tokio::test]
    async fn profile_types_are_replayed_from_the_series_rather_than_stored() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = seed();
        seed_partitioned_block(&mut index);
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        check!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
        let cpu = profile_labels("process_cpu", CPU_TYPE, "checkout");
        check!(
            loaded.fingerprints_for_profile_type("t", CPU_TYPE)
                == BTreeSet::from([cpu.fingerprint()])
        );
    }

    #[tokio::test]
    async fn load_rejects_over_cap_snapshot() {
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let snapshot_key = index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap object; the real manifest is well above 1 byte.
        let size = store
            .head(&Path::from(snapshot_key.clone()))
            .await
            .unwrap()
            .size;
        assert2::assert!(size > 1);

        let got = ProfileIndex::load_latest_snapshot_with_max_bytes(
            &store,
            "index/profiles.json",
            krabka_units::bytes(1),
        )
        .await;
        let Err(BlockStoreError::InvalidBlock(msg)) = got else {
            panic!("expected InvalidBlock for oversized profile index snapshot");
        };
        assert2::assert!(
            msg == format!(
                "profile index snapshot `{snapshot_key}` is {size} bytes, exceeds cap of 1 bytes"
            )
        );

        // A cap at/above the real size still loads.
        let loaded = ProfileIndex::load_latest_snapshot_with_max_bytes(
            &store,
            "index/profiles.json",
            crate::DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await
        .unwrap();
        let mut profile_types = loaded.profile_types("t");
        profile_types.sort();
        assert2::assert!(profile_types == strings(&[HEAP_TYPE, CPU_TYPE]));
    }

    /// A payload the manifest names is not allowed to be missing.
    ///
    /// The orphan sweep leaves anything a retained manifest names alone, so an
    /// absent payload is a torn write or an outside deletion. Answering from
    /// the shards that are left would hide a block that exists.
    #[tokio::test]
    async fn load_missing_shard_payload_preserves_object_store_error_text() {
        use futures::StreamExt as _;
        use object_store::memory::InMemory;

        let mut index = seed();
        seed_partitioned_block(&mut index);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let prefix = Path::from("index/profiles/payloads");
        let payload = store
            .list(Some(&prefix))
            .next()
            .await
            .expect("the manifest names at least one payload")
            .unwrap()
            .location;
        store.delete(&payload).await.unwrap();
        let expected = store.head(&payload).await.unwrap_err().to_string();

        let got = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json").await;

        let Err(BlockStoreError::ObjectStore(msg)) = got else {
            panic!("expected ObjectStore error for a missing profile shard payload");
        };
        assert_eq!(msg, expected);
    }

    #[test]
    fn a_compacted_profile_block_records_its_level_on_its_own_record() {
        let (mut index, cpu_checkout_fp, heap_checkout_fp, _) = seed_with_blocks();
        index.add_profile_block("t", "cpu-checkout.parquet", vec![1]);
        check!(index.block_level("cpu-checkout.parquet") == BlockLevel::INGESTED);

        let level = index.replace_profile_blocks(
            "t",
            &strings(&["cpu-checkout.parquet", "heap-checkout.parquet"]),
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "compacted.parquet".to_string(),
                    min_ts: 100,
                    max_ts: 399,
                    row_count: 30,
                    fingerprints: vec![cpu_checkout_fp, heap_checkout_fp],
                    // What the writer stamped. The index promotes it, so
                    // handing over a level here cannot get the ladder wrong.
                    level: BlockLevel::INGESTED,
                },
                vec![9],
            )],
        );

        check!(level == BlockLevel(1));
        check!(index.block_level("compacted.parquet") == BlockLevel(1));
        // The inputs are gone, and the level went with the records rather than
        // outliving them in a map of its own.
        check!(index.block_level("cpu-checkout.parquet") == BlockLevel::INGESTED);
        check!(
            index
                .all_blocks()
                .iter()
                .all(|meta| meta.object_key != "cpu-checkout.parquet")
        );
    }

    /// A pending removal pins itself to the fingerprint of the record it
    /// retired. The level is part of that record, so a block and its compacted
    /// replacement under one key must not agree on a fingerprint -- a removal
    /// that matched both would drop a live object nothing else names.
    #[test]
    fn the_record_fingerprint_tells_two_profile_blocks_apart_by_level() {
        let ingested = BlockMeta {
            tenant: "t".to_string(),
            object_key: "cpu-checkout.parquet".to_string(),
            min_ts: 100,
            max_ts: 199,
            row_count: 10,
            fingerprints: vec![7],
            level: BlockLevel::INGESTED,
        };
        let compacted = BlockMeta {
            level: BlockLevel(1),
            ..ingested.clone()
        };

        let of = |meta: &BlockMeta| profile_block_fingerprint(meta, &[1]);
        check!(of(&ingested) == of(&ingested.clone()));
        check!(of(&ingested) != of(&compacted));
    }

    #[test]
    fn profile_compaction_candidates_take_their_bounds_from_the_series_postings() {
        let (index, ..) = seed_with_blocks();
        check!(
            index.compaction_candidates()
                == vec![
                    CompactionCandidate {
                        tenant: "t".to_string(),
                        object_key: "cpu-checkout.parquet".to_string(),
                        min_ts: 100,
                        max_ts: 199,
                        row_count: 10,
                        level: BlockLevel::INGESTED,
                    },
                    CompactionCandidate {
                        tenant: "t".to_string(),
                        object_key: "cpu-payments.parquet".to_string(),
                        min_ts: 150,
                        max_ts: 250,
                        row_count: 30,
                        level: BlockLevel::INGESTED,
                    },
                    CompactionCandidate {
                        tenant: "t".to_string(),
                        object_key: "heap-checkout.parquet".to_string(),
                        min_ts: 300,
                        max_ts: 399,
                        row_count: 20,
                        level: BlockLevel::INGESTED,
                    },
                ]
        );
    }

    /// A level that did not survive the snapshot would restart the ladder at
    /// zero after every save, and the planner would compact the same rows for
    /// as long as the compactor ran.
    #[tokio::test]
    async fn profile_levels_survive_a_snapshot_round_trip() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (mut index, cpu_checkout_fp, heap_checkout_fp, _) = seed_with_blocks();
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        index.replace_profile_blocks(
            "t",
            &strings(&["cpu-checkout.parquet", "heap-checkout.parquet"]),
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "compacted.parquet".to_string(),
                    min_ts: 100,
                    max_ts: 399,
                    row_count: 30,
                    fingerprints: vec![cpu_checkout_fp, heap_checkout_fp],
                    level: BlockLevel::INGESTED,
                },
                vec![9],
            )],
        );
        index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        check!(loaded.block_level("compacted.parquet") == BlockLevel(1));
        check!(loaded.block_level("cpu-payments.parquet") == BlockLevel::INGESTED);
        check!(
            loaded
                .all_blocks()
                .iter()
                .all(|meta| meta.object_key != "cpu-checkout.parquet")
        );
    }

    /// A series with no `__profile_type__` is refused, and the refusal names
    /// both the label it wants and the series it is about.
    ///
    /// What this replaces was worse than an error. `add_series` returned
    /// having registered the series' labels and no profile type, so the
    /// profile stored without complaint and every profile-type selector
    /// missed it: a write that could never be read, reported at neither end.
    #[test]
    fn a_series_without_a_profile_type_label_is_refused_by_name() {
        let mut index = ProfileIndex::new();
        let untyped = labels(&[("__name__", "process_cpu"), ("service_name", "checkout")]);

        let error = index
            .add_series("t", untyped.fingerprint(), &untyped)
            .expect_err("a series with no profile type is unqueryable and must be refused");

        let BlockStoreError::MissingProfileTypeLabel {
            label,
            tenant,
            fingerprint,
            labels: series,
        } = &error
        else {
            panic!("wrong variant for a missing profile-type label: {error}");
        };
        check!(
            (*label, tenant.as_str(), *fingerprint, series.as_str())
                == (
                    "__profile_type__",
                    "t",
                    untyped.fingerprint(),
                    r#"__name__="process_cpu", service_name="checkout""#,
                )
        );
        // The whole rendered message, because what the operator has to be able
        // to do from a log line alone is find the series and the label.
        check!(
            error.to_string()
                == format!(
                    r#"profile series {{__name__="process_cpu", service_name="checkout"}} of tenant `t` (fingerprint {}) has no `__profile_type__` label, so no profile-type selector could ever reach it"#,
                    untyped.fingerprint()
                )
        );
    }

    /// A refused series leaves nothing of itself behind.
    ///
    /// Half of the old behaviour was the postings: the series went into the
    /// label index and only the profile type was skipped, so `label_values`
    /// listed a service whose profiles no query could return.
    #[test]
    fn a_refused_series_registers_neither_labels_nor_a_profile_type() {
        let mut index = ProfileIndex::new();
        let untyped = labels(&[("__name__", "process_cpu"), ("service_name", "checkout")]);

        check!(
            index
                .add_series("t", untyped.fingerprint(), &untyped)
                .is_err()
        );

        check!(index.label_names("t") == Vec::<String>::new());
        check!(index.profile_types("t") == Vec::<String>::new());
        check!(
            index
                .matching_fingerprints(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::new()
        );
    }

    /// The control: the same series, with the label, is registered under its
    /// type and comes back from a type selector.
    #[test]
    fn a_series_with_a_profile_type_label_is_selectable_by_that_type() {
        let mut index = ProfileIndex::new();
        let typed = profile_labels("process_cpu", CPU_TYPE, "checkout");

        index
            .add_series("t", typed.fingerprint(), &typed)
            .expect("a series carrying __profile_type__ is registrable");

        check!(index.profile_types("t") == strings(&[CPU_TYPE]));
        check!(
            index
                .select_fingerprints(
                    "t",
                    CPU_TYPE,
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::from([typed.fingerprint()])
        );
    }
}

mod decode_profile_shard;
mod encode_profile_shard;
mod label_profile_type;
mod max_profile_index_snapshot_bytes;
mod profile_block_fingerprint;
mod profile_index_shard_width;
mod profile_index_type;
mod profile_shard;
mod profile_shard_format;
mod render_series_labels;
mod tenant_profile_extras;

use decode_profile_shard::decode_profile_shard;
use encode_profile_shard::encode_profile_shard;
pub use label_profile_type::LABEL_PROFILE_TYPE;
pub use max_profile_index_snapshot_bytes::MAX_PROFILE_INDEX_SNAPSHOT_BYTES;
use profile_block_fingerprint::profile_block_fingerprint;
use profile_index_shard_width::PROFILE_INDEX_SHARD_WIDTH;
pub use profile_index_type::ProfileIndex;
use profile_shard::ProfileShard;
use profile_shard_format::{PROFILE_SHARD_FORMAT_VERSION, PROFILE_SHARD_MAGIC};
use render_series_labels::render_series_labels;
use tenant_profile_extras::TenantProfileExtras;
