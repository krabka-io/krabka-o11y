use std::collections::BTreeSet;

use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogBlockStoreError,
    SeriesFingerprint, TimeRange, delete_tenant_log_index_shard_from_object_store, labels,
    log_tenant_index_manifest_object_path, log_tenant_index_shard_catalog_object_path,
    log_tenant_index_shard_manifest_object_path, read_log_index_manifest,
    read_log_index_manifest_from_object_store, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, read_tenant_log_index_shards_from_object_store,
    write_log_index_manifest, write_log_index_manifest_to_object_store,
    write_tenant_log_index_manifest_to_object_store, write_tenant_log_index_shard_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use object_store::{
    ObjectStoreExt as _, PutPayload, local::LocalFileSystem, path::Path as ObjectPath,
};

// Where a test block sits in the WAL and in time.
#[derive(Clone, Copy)]
struct BlockSlot {
    first_offset: i64,
    last_offset: i64,
    time: TimeRange,
}

const EARLY: BlockSlot = BlockSlot {
    first_offset: 10,
    last_offset: 19,
    time: TimeRange {
        start_ns: 100,
        end_ns: 199,
    },
};
const MIDDLE: BlockSlot = BlockSlot {
    first_offset: 20,
    last_offset: 29,
    time: TimeRange {
        start_ns: 200,
        end_ns: 299,
    },
};
const LATE: BlockSlot = BlockSlot {
    first_offset: 30,
    last_offset: 39,
    time: TimeRange {
        start_ns: 400,
        end_ns: 499,
    },
};

// An `{app, env="prod"}` series of one tenant.
#[derive(Clone, Copy)]
struct TestSeries<'a> {
    tenant: &'a str,
    app: &'a str,
}

fn series(index: &mut LabelIndex, of: TestSeries<'_>) -> SeriesFingerprint {
    index.insert_series(of.tenant, labels([("app", of.app), ("env", "prod")]))
}

// A block of one tenant on partition 0 that carries one series.
#[derive(Clone, Copy)]
struct TestBlock<'a> {
    tenant: &'a str,
    slot: BlockSlot,
    series: SeriesFingerprint,
}

fn insert_block(blocks: &mut BlockIndex, block: TestBlock<'_>) {
    let slot = block.slot;
    blocks.insert(BlockDescriptor::new(
        BlockKey::new(
            block.tenant,
            0,
            slot.first_offset,
            slot.last_offset,
            slot.time,
        ),
        BTreeSet::from([block.series]),
    ));
}

// One tenant-a block per slot, for api, worker and admin; returns admin.
fn api_worker_admin_blocks(labels_index: &mut LabelIndex) -> (BlockIndex, SeriesFingerprint) {
    let mut blocks = BlockIndex::default();
    let mut admin = 0;
    for (slot, app) in [(EARLY, "api"), (MIDDLE, "worker"), (LATE, "admin")] {
        admin = series(
            labels_index,
            TestSeries {
                tenant: "tenant-a",
                app,
            },
        );
        insert_block(
            &mut blocks,
            TestBlock {
                tenant: "tenant-a",
                slot,
                series: admin,
            },
        );
    }
    (blocks, admin)
}

// Writes one tenant-a shard holding one api block, and returns its range.
async fn write_one_api_shard(store: &LocalFileSystem, prefix: &ObjectPath) -> TimeRange {
    let mut labels_index = LabelIndex::default();
    let api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "api",
        },
    );
    let mut blocks = BlockIndex::default();
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: EARLY,
            series: api,
        },
    );
    let shard_range = TimeRange::new(100, 199).unwrap();
    write_tenant_log_index_shards_to_object_store(
        store,
        prefix,
        "tenant-a",
        &[shard_range],
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    shard_range
}

#[test]
fn log_index_manifest_round_trips_label_and_block_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let mut labels_index = LabelIndex::default();
    let api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "api",
        },
    );
    let worker = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "worker",
        },
    );
    series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-b",
            app: "api",
        },
    );

    let mut blocks = BlockIndex::default();
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: EARLY,
            series: api,
        },
    );
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: MIDDLE,
            series: worker,
        },
    );

    write_log_index_manifest(dir.path(), &labels_index, &blocks).unwrap();
    let (loaded_labels, loaded_blocks) = read_log_index_manifest(dir.path()).unwrap();

    assert2::assert!(loaded_labels == labels_index);
    assert2::assert!(loaded_blocks == blocks);
}

#[tokio::test]
async fn log_index_manifest_round_trips_through_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "api",
        },
    );
    let worker = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "worker",
        },
    );

    let mut blocks = BlockIndex::default();
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: EARLY,
            series: api,
        },
    );
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: MIDDLE,
            series: worker,
        },
    );

    write_log_index_manifest_to_object_store(&store, &prefix, &labels_index, &blocks)
        .await
        .unwrap();
    let (loaded_labels, loaded_blocks) = read_log_index_manifest_from_object_store(&store, &prefix)
        .await
        .unwrap();

    assert2::assert!(loaded_labels == labels_index);
    assert2::assert!(loaded_blocks == blocks);
}

#[test]
fn tenant_log_index_manifest_object_path_is_tenant_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert2::assert!(
        log_tenant_index_manifest_object_path(&prefix, "tenant-a").to_string()
            == "observability/logs/tenant=tenant-a/index/logs/manifest.json"
    );
}

#[test]
fn tenant_log_index_shard_manifest_object_path_is_tenant_and_time_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert2::assert!(
        log_tenant_index_shard_manifest_object_path(
            &prefix,
            "tenant-a",
            TimeRange::new(100, 199).unwrap(),
        )
        .to_string()
            == "observability/logs/tenant=tenant-a/index/logs/shards/time=100-199/manifest.json"
    );
}

#[test]
fn tenant_log_index_shard_catalog_object_path_is_tenant_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert2::assert!(
        log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a").to_string()
            == "observability/logs/tenant=tenant-a/index/logs/shards/manifest.json"
    );
}

#[tokio::test]
async fn tenant_log_index_manifest_round_trips_only_one_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let selected_api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-a",
            app: "api",
        },
    );
    let other_tenant_api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-b",
            app: "api",
        },
    );

    let mut blocks = BlockIndex::default();
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-a",
            slot: EARLY,
            series: selected_api,
        },
    );
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-b",
            slot: EARLY,
            series: other_tenant_api,
        },
    );

    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    let expected_selected = blocks.match_blocks(
        "tenant-a",
        TimeRange::new(0, 1_000).unwrap(),
        &[selected_api],
    );

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(loaded_labels.label_values("tenant-b", "app") == BTreeSet::new());
    assert2::assert!(
        loaded_blocks.match_blocks(
            "tenant-a",
            TimeRange::new(0, 1_000).unwrap(),
            &[selected_api],
        ) == expected_selected
    );
    assert2::assert!(
        loaded_blocks.match_blocks(
            "tenant-b",
            TimeRange::new(0, 1_000).unwrap(),
            &[other_tenant_api],
        ) == Vec::new()
    );
}

#[tokio::test]
async fn tenant_log_index_shard_round_trips_only_matching_time_and_series() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let (mut blocks, admin) = api_worker_admin_blocks(&mut labels_index);
    let other_tenant_api = series(
        &mut labels_index,
        TestSeries {
            tenant: "tenant-b",
            app: "api",
        },
    );
    insert_block(
        &mut blocks,
        TestBlock {
            tenant: "tenant-b",
            slot: EARLY,
            series: other_tenant_api,
        },
    );

    let shard_range = TimeRange::new(150, 250).unwrap();
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        shard_range,
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) =
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
            .await
            .unwrap();
    let expected_blocks = blocks.match_blocks("tenant-a", TimeRange::new(150, 250).unwrap(), &[]);

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app")
            == BTreeSet::from(["api".into(), "worker".into()])
    );
    assert2::assert!(loaded_labels.label_values("tenant-b", "app") == BTreeSet::new());
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 1_000).unwrap(), &[])
            == expected_blocks
    );
    assert2::assert!(loaded_labels.labels_for("tenant-a", admin) == None);
}

#[tokio::test]
async fn tenant_log_index_shard_catalog_selects_overlapping_shards_and_merges_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let (blocks, admin) = api_worker_admin_blocks(&mut labels_index);

    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(100, 199).unwrap(),
            TimeRange::new(200, 299).unwrap(),
            TimeRange::new(400, 499).unwrap(),
        ],
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(150, 250).unwrap(),
    )
    .await
    .unwrap();
    let expected_blocks = blocks.match_blocks("tenant-a", TimeRange::new(150, 250).unwrap(), &[]);

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app")
            == BTreeSet::from(["api".into(), "worker".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 1_000).unwrap(), &[])
            == expected_blocks
    );
    assert2::assert!(loaded_labels.labels_for("tenant-a", admin) == None);
}

/// One shard, its manifest deleted, and a catalog that still names it. This is
/// what a query sees when it races the retention sweep: the sweep deletes the
/// manifest of a shard it empties, and the catalog this read falls back on was
/// written before that delete.
///
/// The read has to answer for the shard rather than fail, because a sweep is
/// allowed to produce a per-block error at worst and never a failed query.
#[tokio::test]
async fn an_absent_shard_manifest_reads_as_an_empty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let shard_range = write_one_api_shard(&store, &prefix).await;

    delete_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
        .await
        .unwrap();
    // A manifest that is already gone is not a fault, so a second sweep over
    // the same shard is not one either.
    delete_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
        .await
        .unwrap();

    let query_range = TimeRange::new(0, 1_000).unwrap();
    let (loaded_labels, loaded_blocks) =
        read_tenant_log_index_shards_from_object_store(&store, &prefix, "tenant-a", query_range)
            .await
            .unwrap();

    assert2::assert!(loaded_labels.label_values("tenant-a", "app") == BTreeSet::new());
    assert2::assert!(loaded_blocks.match_blocks("tenant-a", query_range, &[]) == Vec::new());
}

/// A manifest that is present must still be well-formed. The tolerance above
/// covers an absent object only, so a corrupt index is never read as an empty
/// one.
#[tokio::test]
async fn a_present_shard_manifest_that_is_malformed_or_of_another_version_fails_the_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let shard_range = write_one_api_shard(&store, &prefix).await;
    let manifest_path =
        log_tenant_index_shard_manifest_object_path(&prefix, "tenant-a", shard_range);
    let manifest_path = ObjectPath::from(format!(
        "{}/00000000000000000000.json",
        krabka_blockstore::index_snapshot_prefix_for_key(manifest_path.as_ref())
    ));
    let query_range = TimeRange::new(0, 1_000).unwrap();

    let written = store
        .get(&manifest_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut manifest: serde_json::Value = serde_json::from_slice(&written).unwrap();
    manifest["format_version"] = serde_json::Value::from(2);
    store
        .put(
            &manifest_path,
            serde_json::to_vec(&manifest).unwrap().into(),
        )
        .await
        .unwrap();
    let wrong_version =
        read_tenant_log_index_shards_from_object_store(&store, &prefix, "tenant-a", query_range)
            .await;
    assert2::assert!(
        let Err(LogBlockStoreError::InvalidManifestVersion { actual: 2, expected: 1 }) =
            wrong_version
    );

    store
        .put(&manifest_path, PutPayload::from_static(b"not a manifest"))
        .await
        .unwrap();
    let malformed =
        read_tenant_log_index_shards_from_object_store(&store, &prefix, "tenant-a", query_range)
            .await;
    assert2::assert!(let Err(LogBlockStoreError::Json(_)) = malformed);
}
