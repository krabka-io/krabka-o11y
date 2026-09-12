use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use assert2::{assert, check};
use futures::{StreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    BlockDeletion, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, Index, Labels, ProfileIndex,
    ShardedTraceBloom, TraceBlockStats, TraceIndex, delete_blocks, reconcile_orphans,
};
use krabka_units::{Time, convert::TimeExt as _, hours};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    local::LocalFileSystem, memory::InMemory, path::Path,
};

const CPU_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn block_deletion(object_key: &str) -> BlockDeletion {
    BlockDeletion {
        object_key: object_key.to_string(),
        sidecars: Vec::new(),
    }
}

fn trace_stats(object_key: &str, min_ts: i64, max_ts: i64) -> TraceBlockStats {
    TraceBlockStats {
        object_key: object_key.to_string(),
        min_ts,
        max_ts,
        bloom: ShardedTraceBloom::match_all_with_tempo_defaults(),
        tag_names: BTreeSet::new(),
        tag_values: BTreeMap::new(),
        row_count: 1,
        level: BlockLevel::INGESTED,
    }
}

fn trace_keys(index: &TraceIndex) -> Vec<String> {
    let mut keys: Vec<String> = index
        .trace_blocks("t")
        .iter()
        .map(|block| block.object_key.clone())
        .collect();
    keys.sort();
    keys
}

fn profile_keys(index: &ProfileIndex) -> Vec<String> {
    let mut keys: Vec<String> = index
        .all_blocks()
        .into_iter()
        .map(|meta| meta.object_key)
        .collect();
    keys.sort();
    keys
}

fn meta(object_key: &str, min_ts: i64, max_ts: i64, fingerprints: Vec<u64>) -> BlockMeta {
    BlockMeta {
        tenant: "t".to_string(),
        object_key: object_key.to_string(),
        min_ts,
        max_ts,
        row_count: 1,
        fingerprints,
        level: BlockLevel::INGESTED,
    }
}

async fn put_all(store: &dyn ObjectStore, keys: &[&str]) {
    for key in keys {
        store
            .put(&Path::from(*key), PutPayload::from_static(b"x"))
            .await
            .unwrap();
    }
}

async fn listed_keys(store: &dyn ObjectStore) -> Vec<String> {
    let mut keys = store
        .list(None)
        .map(|meta| meta.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    keys.sort();
    keys
}

/// The merge that publishes a trace index is a union, so a removal has to be
/// replayed against the merge base. Without the replay every snapshot write
/// after a retention sweep would resurrect the blocks the sweep dropped.
#[tokio::test]
async fn a_removed_trace_block_does_not_come_back_on_the_next_snapshot() {
    const KEY: &str = "index/traces.json";
    let store = store();
    let mut index = TraceIndex::new();
    index.add_trace_block("t", trace_stats("b1", 0, 100));
    index.add_trace_block("t", trace_stats("b2", 200, 300));
    index.save_latest_snapshot(&store, KEY).await.unwrap();

    let removed = index.remove_trace_blocks("t", &strings(&["b1"]));
    index.save_latest_snapshot(&store, KEY).await.unwrap();

    check!(removed == 1);
    let loaded = TraceIndex::load_latest_snapshot(&store, KEY).await.unwrap();
    check!(trace_keys(&loaded) == strings(&["b2"]));

    // The removal is durable now, so the next write need not replay it and
    // must not undo it either.
    index.save_latest_snapshot(&store, KEY).await.unwrap();
    let reloaded = TraceIndex::load_latest_snapshot(&store, KEY).await.unwrap();
    check!(trace_keys(&reloaded) == strings(&["b2"]));

    // A key the index no longer holds is not a failure.
    check!(index.remove_trace_blocks("t", &strings(&["b1"])) == 0);
    check!(index.remove_trace_blocks("absent", &strings(&["b2"])) == 0);
}

/// Object keys are derived rather than minted, so the same key can be written
/// again after a writer dropped it.
///
/// A removal recorded by name alone would hide that live block, and hide it for
/// good once the base carried the drop forward. The removal pins itself to the
/// record it retired, so it fails to match the newer record and the stale
/// writer's save is refused instead of silently losing an object.
#[tokio::test]
async fn a_removal_does_not_hide_a_live_block_written_under_the_same_key() {
    const KEY: &str = "index/traces.json";
    let store = store();
    let mut stale = TraceIndex::new();
    stale.add_trace_block("t", trace_stats("b1", 0, 100));
    stale.save_latest_snapshot(&store, KEY).await.unwrap();

    // Another writer mints the same object key for a different block.
    let mut reuser = TraceIndex::new();
    reuser.add_trace_block("t", trace_stats("b1", 400, 500));
    reuser.save_latest_snapshot(&store, KEY).await.unwrap();

    stale.remove_trace_blocks("t", &strings(&["b1"]));
    let got = stale.save_latest_snapshot(&store, KEY).await;

    assert!(matches!(
        got,
        Err(BlockStoreError::InvalidBlock(message)) if message.contains("b1")
    ));
    let loaded = TraceIndex::load_latest_snapshot(&store, KEY).await.unwrap();
    check!(trace_keys(&loaded) == strings(&["b1"]));
    // The record under the reused key is the live one, not the retired one.
    check!(
        loaded
            .trace_blocks("t")
            .iter()
            .find(|block| block.object_key == "b1")
            .map(|block| block.min_ts)
            == Some(400)
    );
}

#[tokio::test]
async fn a_removed_profile_block_takes_its_partitions_and_does_not_come_back() {
    const KEY: &str = "index/profiles.json";
    let store = store();
    let mut index = ProfileIndex::new();
    let labels = Labels::from_pairs([
        ("__name__", "process_cpu".to_string()),
        ("__profile_type__", CPU_TYPE.to_string()),
        ("service_name", "checkout".to_string()),
    ]);
    let fingerprint = labels.fingerprint();
    index.add_series("t", fingerprint, &labels).unwrap();
    for (object_key, min_ts, max_ts) in [("p1.parquet", 0, 100), ("p2.parquet", 200, 300)] {
        BlockIndex::add_block(
            &mut index,
            &meta(object_key, min_ts, max_ts, vec![fingerprint]),
        );
        index.add_profile_block("t", object_key, vec![7]);
    }
    index.save_latest_snapshot(&store, KEY).await.unwrap();

    let removed = index.remove_profile_blocks("t", &strings(&["p1.parquet"]));
    index.save_latest_snapshot(&store, KEY).await.unwrap();

    check!(removed == 1);
    check!(index.stacktrace_partitions("p1.parquet").is_empty());
    let loaded = ProfileIndex::load_latest_snapshot(&store, KEY)
        .await
        .unwrap();
    check!(profile_keys(&loaded) == strings(&["p2.parquet"]));
    check!(loaded.stacktrace_partitions("p1.parquet").is_empty());
    check!(loaded.stacktrace_partitions("p2.parquet") == vec![7]);
}

/// The metrics index republishes every shard it names and sweeps the shard
/// objects the new layout does not, so its removal needs no replay of its own.
#[tokio::test]
async fn a_removed_metrics_block_is_gone_after_the_next_save() {
    const KEY: &str = "index/metrics.json";
    let store = store();
    let labels = Labels::from_pairs([("app", "api".to_string())]);
    let fingerprint = labels.fingerprint();
    let mut index = Index::new();
    index.add_series("t", fingerprint, &labels);
    index.add_block(&meta("b1.parquet", 0, 50, vec![fingerprint]));
    index.add_block(&meta("b2.parquet", 1_000, 1_050, vec![fingerprint]));
    index.save_with_shard_width(&store, KEY, 100).await.unwrap();

    let removed = index.remove_blocks("t", &strings(&["b1.parquet"]));
    index.save_with_shard_width(&store, KEY, 100).await.unwrap();

    check!(removed == 1);
    // Removing again, and removing from a tenant the index never held, are
    // both no-ops rather than failures.
    check!(index.remove_blocks("t", &strings(&["b1.parquet"])) == 0);
    check!(index.remove_blocks("absent", &strings(&["b2.parquet"])) == 0);
    let loaded = Index::load(&store, KEY).await.unwrap();
    check!(loaded.blocks_in_range("t", 0, 2_000) == strings(&["b2.parquet"]));
}

/// A profiles block has a `.symdb` beside it and a metrics block a `.index`.
/// Nothing but the block names them, so a deletion that took the block alone
/// would leave the sidecar on object storage for as long as the bucket lives.
#[tokio::test]
async fn deleting_a_block_deletes_its_sidecars_too() {
    let store = store();
    put_all(
        &store,
        &[
            "blocks/b1.parquet",
            "blocks/b1.parquet.symdb",
            "blocks/b2.parquet",
            "blocks/b2.parquet.index",
            "blocks/keep.parquet",
        ],
    )
    .await;

    let report = delete_blocks(
        &store,
        &[
            BlockDeletion {
                object_key: "blocks/b1.parquet".to_string(),
                sidecars: strings(&["blocks/b1.parquet.symdb"]),
            },
            BlockDeletion {
                object_key: "blocks/b2.parquet".to_string(),
                sidecars: strings(&["blocks/b2.parquet.index"]),
            },
        ],
    )
    .await;

    check!(report.blocks_deleted == 2);
    check!(report.sidecars_deleted == 2);
    check!(report.objects_absent == 0);
    check!(report.failures.is_empty());
    check!(listed_keys(&store).await == strings(&["blocks/keep.parquet"]));
}

/// An object that is already gone counts as success rather than as a failure.
/// A sweep that was interrupted, and a sweep that simply runs twice, both
/// reach objects a previous pass already deleted.
///
/// The backend here is `LocalFileSystem` because `InMemory` removes a missing
/// key silently and reports success, so it cannot exercise the tolerance at
/// all. A real backend reports `NotFound`, and this is the store in the
/// workspace that does.
#[tokio::test]
async fn an_object_that_is_already_gone_counts_as_success() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    put_all(&store, &["blocks/present.parquet"]).await;

    let report = delete_blocks(
        &store,
        &[
            BlockDeletion {
                object_key: "blocks/present.parquet".to_string(),
                // Never written, as a block whose sidecar a previous pass
                // already removed would be.
                sidecars: strings(&["blocks/present.parquet.symdb"]),
            },
            BlockDeletion {
                object_key: "blocks/gone.parquet".to_string(),
                sidecars: Vec::new(),
            },
        ],
    )
    .await;

    check!(report.blocks_deleted == 1);
    check!(report.sidecars_deleted == 0);
    // The missing sidecar and the missing block.
    check!(report.objects_absent == 2);
    check!(report.failures.is_empty());
}

/// A retention sweep covers many tenants, so one object a backend refuses must
/// not strand every block behind it in the list.
#[tokio::test]
async fn one_failed_delete_does_not_stop_the_rest_and_is_reported() {
    let inner = store();
    put_all(
        &inner,
        &[
            "blocks/a.parquet",
            "blocks/refused.parquet",
            "blocks/z.parquet",
        ],
    )
    .await;
    let store: Arc<dyn ObjectStore> = Arc::new(RefusingStore {
        inner: Arc::clone(&inner),
        refuse: "blocks/refused.parquet".to_string(),
    });

    let report = delete_blocks(
        &store,
        &[
            BlockDeletion {
                object_key: "blocks/a.parquet".to_string(),
                sidecars: Vec::new(),
            },
            BlockDeletion {
                object_key: "blocks/refused.parquet".to_string(),
                sidecars: Vec::new(),
            },
            BlockDeletion {
                object_key: "blocks/z.parquet".to_string(),
                sidecars: Vec::new(),
            },
        ],
    )
    .await;

    // The block listed after the refusal is still deleted.
    check!(report.blocks_deleted == 2);
    check!(report.failures.len() == 1);
    check!(report.failures[0].object_key == "blocks/refused.parquet");
    check!(report.failures[0].failed_key == "blocks/refused.parquet");
    check!(!report.failures[0].error.is_empty());
    check!(listed_keys(&inner).await == strings(&["blocks/refused.parquet"]));
}

/// A caller that holds its store as a plain `&dyn ObjectStore` gets the same
/// two tolerances as one that holds an `Arc`: an object already gone counts as
/// success, and one object a backend refuses does not strand the blocks listed
/// behind it.
///
/// The backend under the refusal is `LocalFileSystem` because `InMemory`
/// removes a missing key silently and reports success, so it cannot exercise
/// the `NotFound` tolerance at all.
#[tokio::test]
async fn a_borrowed_store_deletes_blocks_with_the_same_tolerances() {
    let dir = tempfile::tempdir().unwrap();
    // Owned by the test and never wrapped, so the borrow is the whole of what
    // `delete_blocks` is given.
    let store = RefusingStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        refuse: "blocks/refused.parquet".to_string(),
    };
    put_all(
        &store,
        &[
            "blocks/a.parquet",
            "blocks/refused.parquet",
            "blocks/z.parquet",
        ],
    )
    .await;

    let report = delete_blocks(
        &store,
        &[
            block_deletion("blocks/a.parquet"),
            block_deletion("blocks/refused.parquet"),
            // Never written, as a block a previous pass already deleted would be.
            block_deletion("blocks/gone.parquet"),
            block_deletion("blocks/z.parquet"),
        ],
    )
    .await;

    check!(report.blocks_deleted == 2);
    check!(report.objects_absent == 1);
    check!(report.sidecars_deleted == 0);
    check!(report.failures.len() == 1);
    check!(report.failures[0].object_key == "blocks/refused.parquet");
    check!(report.failures[0].failed_key == "blocks/refused.parquet");
    check!(listed_keys(&store).await == strings(&["blocks/refused.parquet"]));
}

/// The orphan sweep takes the same borrowed store, and it sweeps through it.
#[tokio::test]
async fn a_borrowed_store_sweeps_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    put_all(&store, &["blocks/live.parquet", "blocks/orphan.parquet"]).await;
    let live = BTreeSet::from(["blocks/live.parquet".to_string()]);

    let swept = reconcile_orphans(
        &store,
        "blocks",
        &live,
        Time::ZERO,
        SystemTime::now() + Duration::from_mins(1),
    )
    .await
    .unwrap();

    check!(swept.listed == 2);
    check!(swept.live == 1);
    check!(swept.deleted == 1);
    check!(swept.failed == 0);
    check!(listed_keys(&store).await == strings(&["blocks/live.parquet"]));
}

/// The sweep deletes what the index does not name, keeps what it does, and
/// leaves a freshly written object alone whoever wrote it.
#[tokio::test]
async fn the_orphan_sweep_deletes_only_dated_unreferenced_objects() {
    let store = store();
    put_all(
        &store,
        &[
            "blocks/live.parquet",
            "blocks/orphan.parquet",
            "blocks/just-written.parquet",
        ],
    )
    .await;
    let live = BTreeSet::from(["blocks/live.parquet".to_string()]);

    // Everything here was written moments ago. A block is written before its
    // index entry is saved, so an unreferenced object this new is
    // indistinguishable from one a writer is about to publish.
    let held = reconcile_orphans(&store, "blocks", &live, hours(1), SystemTime::now())
        .await
        .unwrap();

    check!(held.listed == 3);
    check!(held.live == 1);
    check!(held.kept_within_grace == 2);
    check!(held.deleted == 0);
    check!(listed_keys(&store).await.len() == 3);

    // Once the grace window has elapsed the unreferenced objects go, and the
    // one the index names stays.
    let swept = reconcile_orphans(
        &store,
        "blocks",
        &live,
        Time::ZERO,
        SystemTime::now() + Duration::from_mins(1),
    )
    .await
    .unwrap();

    check!(swept.listed == 3);
    check!(swept.live == 1);
    check!(swept.kept_within_grace == 0);
    check!(swept.deleted == 2);
    check!(swept.failed == 0);
    check!(listed_keys(&store).await == strings(&["blocks/live.parquet"]));
}

/// An [`ObjectStore`] that refuses to delete one named object.
///
/// `delete_stream` is the only delete primitive the trait has, and
/// `ObjectStoreExt::delete` routes through it, so refusing here refuses both.
#[derive(Debug)]
struct RefusingStore {
    inner: Arc<dyn ObjectStore>,
    refuse: String,
}

impl std::fmt::Display for RefusingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RefusingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let refuse = self.refuse.clone();
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let refuse = refuse.clone();
                async move {
                    let location = location?;
                    if location.as_ref() == refuse {
                        return Err(object_store::Error::Generic {
                            store: "test",
                            source: "this object refuses to be deleted".into(),
                        });
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }
}
