use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use assert2::{assert, check};
use async_trait::async_trait;
use futures::{TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::BlockLevel;
use krabka_metrics::{
    CompactionIndex, CompactionIndexListing, CompactionIndexManifest, CompactionSeriesLabels,
    DecodedTsdbBlock, MetricBlockKind, TsdbBlockFiles, TsdbBlockMeta, TsdbImportLimits,
    TsdbImportObject, TsdbImportOutcome, TsdbImportRecord, TsdbImportTarget, TsdbPublishError,
    decode_float_samples, decode_native_histograms, decode_tsdb_block,
    enforce_compaction_retention, list_compaction_index, list_compaction_manifests,
    publish_tsdb_import, tsdb_block_sha256,
};
use krabka_units::{Time, secs};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tsdb_fixture::{FIXTURE_MAX_TIME, FIXTURE_MIN_TIME, FIXTURE_ULID, fixture_files};

use self::crashing_store::{CrashPoint, CrashingStore};

#[path = "support/crashing_store.rs"]
mod crashing_store;
#[path = "support/tsdb_fixture.rs"]
mod tsdb_fixture;

const TENANT: &str = "tenant-a";
const OTHER_ULID: &str = "01M3MJXM7R4M5X4Q4CKHW5Q8N1";
const MANIFEST_PREFIX: &str = "metrics";
const RECORD_PREFIX: &str = "mimir-block-uploads";
/// The content hash of the checked-in fixture. The import records and the
/// Parquet keys hold it, so a change here breaks the idempotency of imports
/// that earlier builds made.
const FIXTURE_SHA256: &str = "62593eb43039a3ec287a9d7c86438e77741886b170bf178933676504148f1236";

/// The fixture decoded with or without its tombstones, and its content hash.
fn fixture_block(with_tombstones: bool) -> (DecodedTsdbBlock, String) {
    let files = fixture_files();
    let segments = [files.chunks.as_slice()];
    let block_files = TsdbBlockFiles {
        index: &files.index,
        chunk_segments: &segments,
        tombstones: with_tombstones.then_some(files.tombstones.as_slice()),
    };
    let meta = TsdbBlockMeta {
        min_time: FIXTURE_MIN_TIME,
        max_time: FIXTURE_MAX_TIME,
        external_labels: BTreeMap::new(),
    };
    let block = decode_tsdb_block(TENANT, &meta, block_files, &TsdbImportLimits::default())
        .expect("the fixture decodes");
    (block, tsdb_block_sha256(block_files))
}

fn target<'a>(ulid: &'a str, sha256: &'a str) -> TsdbImportTarget<'a> {
    TsdbImportTarget {
        tenant: TENANT,
        block_ulid: ulid,
        sha256,
        manifest_prefix: MANIFEST_PREFIX,
        record_prefix: RECORD_PREFIX,
    }
}

async fn keys_under(store: &dyn ObjectStore, prefix: &str) -> BTreeSet<String> {
    store
        .list(Some(&Path::from(prefix)))
        .map_ok(|object| object.location.to_string())
        .try_collect()
        .await
        .expect("list the store")
}

async fn all_keys(store: &dyn ObjectStore) -> BTreeSet<String> {
    let mut keys = keys_under(store, MANIFEST_PREFIX).await;
    keys.extend(keys_under(store, RECORD_PREFIX).await);
    keys
}

fn import_directory(ulid: &str, sha256: &str) -> String {
    format!("metrics/{TENANT}/uploaded/{ulid}-{}", &sha256[..16])
}

fn object_keys(ulid: &str, sha256: &str, kind: &str) -> (String, String) {
    let stem = format!("{}/{kind}", import_directory(ulid, sha256));
    (format!("{stem}.parquet"), format!("{stem}.index"))
}

fn marker_key(ulid: &str, sha256: &str) -> String {
    format!("{}/_published", import_directory(ulid, sha256))
}

fn expected_record(ulid: &str, sha256: &str, block: &DecodedTsdbBlock) -> TsdbImportRecord {
    let (float_block, float_index) = object_keys(ulid, sha256, "float");
    let (histogram_block, histogram_index) = object_keys(ulid, sha256, "native-histograms");
    TsdbImportRecord {
        version: 1,
        ulid: ulid.to_owned(),
        sha256: sha256.to_owned(),
        stats: block.stats,
        objects: vec![
            TsdbImportObject {
                kind: MetricBlockKind::Float,
                block_key: float_block,
                index_key: float_index,
                rows: u64::try_from(block.rows.float_rows.len()).expect("row count"),
            },
            TsdbImportObject {
                kind: MetricBlockKind::NativeHistograms,
                block_key: histogram_block,
                index_key: histogram_index,
                rows: u64::try_from(block.rows.histogram_rows.len()).expect("row count"),
            },
        ],
        published: true,
    }
}

fn expected_manifest(
    object: &TsdbImportObject,
    block: &DecodedTsdbBlock,
) -> CompactionIndexManifest {
    let samples: Vec<(u64, i64)> = match object.kind {
        MetricBlockKind::Float => block
            .rows
            .float_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms))
            .collect(),
        _ => block
            .rows
            .histogram_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms))
            .collect(),
    };
    let fingerprints: BTreeSet<u64> = samples
        .iter()
        .map(|(fingerprint, _)| *fingerprint)
        .collect();
    CompactionIndexManifest {
        tenant: TENANT.to_owned(),
        kind: object.kind,
        block_key: object.block_key.clone(),
        index_key: object.index_key.clone(),
        level: BlockLevel::INGESTED,
        first_offset: 0,
        last_offset: 0,
        row_count: samples.len(),
        min_ts: samples.iter().map(|(_, ts)| *ts).min().expect("samples"),
        max_ts: samples.iter().map(|(_, ts)| *ts).max().expect("samples"),
        fingerprints: fingerprints.iter().copied().collect(),
        series: fingerprints
            .iter()
            .map(|fingerprint| CompactionSeriesLabels {
                fingerprint: *fingerprint,
                labels: block.rows.series_labels[fingerprint].clone(),
            })
            .collect(),
    }
}

#[test]
fn the_fixture_content_hash_is_stable() {
    let (_, sha256) = fixture_block(true);

    check!(sha256 == FIXTURE_SHA256);
}

#[test]
fn the_content_hash_changes_when_bytes_move_between_files() {
    let files = fixture_files();
    let whole = [files.chunks.as_slice()];
    let (head, tail) = files.chunks.split_at(files.chunks.len() / 2);
    let split = [head, tail];
    let hash = |segments: &[&[u8]], tombstones: Option<&[u8]>| {
        tsdb_block_sha256(TsdbBlockFiles {
            index: &files.index,
            chunk_segments: segments,
            tombstones,
        })
    };
    let hashes = [
        hash(&whole, Some(&files.tombstones)),
        hash(&split, Some(&files.tombstones)),
        hash(&whole, None),
    ];

    check!(hashes.iter().collect::<BTreeSet<_>>().len() == hashes.len());
}

#[tokio::test]
async fn an_import_publishes_blocks_that_read_back_as_the_decoded_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (block, sha256) = fixture_block(true);

    let outcome = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the import publishes");

    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    check!(outcome == TsdbImportOutcome::Imported(record.clone()));
    let manifests = list_compaction_manifests(&store)
        .await
        .expect("list manifests");
    let expected: Vec<_> = record
        .objects
        .iter()
        .map(|object| expected_manifest(object, &block))
        .collect();
    check!(manifests == expected);

    let float_batches = krabka_blockstore::read_block(store.clone(), &record.objects[0].block_key)
        .await
        .expect("read the float block");
    let floats: Vec<_> = float_batches
        .iter()
        .flat_map(|batch| decode_float_samples(batch).expect("decode floats"))
        .map(|(fingerprint, ts, value, start)| (fingerprint, ts, value.to_bits(), start))
        .collect();
    let expected_floats: Vec<_> = block
        .rows
        .float_rows
        .iter()
        .map(|row| {
            (
                row.fingerprint,
                row.timestamp_ms,
                row.value.to_bits(),
                row.start_timestamp_ms,
            )
        })
        .collect();
    check!(floats == expected_floats);

    let histogram_batches =
        krabka_blockstore::read_block(store.clone(), &record.objects[1].block_key)
            .await
            .expect("read the histogram block");
    let histograms: Vec<_> = histogram_batches
        .iter()
        .flat_map(|batch| decode_native_histograms(batch).expect("decode histograms"))
        .collect();
    let expected_histograms: Vec<_> = block
        .rows
        .histogram_rows
        .iter()
        .map(|row| (row.fingerprint, row.timestamp_ms, row.hist.clone()))
        .collect();
    check!(histograms == expected_histograms);
}

#[tokio::test]
async fn a_repeated_import_reports_the_existing_content_and_writes_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (block, sha256) = fixture_block(true);
    publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the first import publishes");
    let before = all_keys(store.as_ref()).await;

    let outcomes = [
        publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the same ULID imports again"),
        publish_tsdb_import(&store, target(OTHER_ULID, &sha256), &block)
            .await
            .expect("another ULID imports the same content"),
    ];

    let existing =
        TsdbImportOutcome::AlreadyImported(expected_record(FIXTURE_ULID, &sha256, &block));
    check!(outcomes == [existing.clone(), existing]);
    let mut expected_keys = before;
    // The second ULID is bound to the content, so a third upload under it
    // with other content conflicts.
    expected_keys.insert(format!("{RECORD_PREFIX}/{TENANT}/{OTHER_ULID}/import.json"));
    check!(all_keys(store.as_ref()).await == expected_keys);
    check!(
        list_compaction_manifests(&store)
            .await
            .expect("list manifests")
            .len()
            == 2
    );
}

#[tokio::test]
async fn other_content_under_an_imported_ulid_conflicts_and_writes_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (block, sha256) = fixture_block(true);
    publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the first import publishes");
    let before = all_keys(store.as_ref()).await;
    let (other_block, other_sha256) = fixture_block(false);

    let error = publish_tsdb_import(&store, target(FIXTURE_ULID, &other_sha256), &other_block)
        .await
        .expect_err("other content under the same ULID conflicts");

    assert!(let TsdbPublishError::Conflict { ulid, existing, uploaded } = error);
    check!((ulid.as_str(), existing, uploaded) == (FIXTURE_ULID, sha256, other_sha256));
    check!(all_keys(store.as_ref()).await == before);
}

/// A store that refuses the writes whose key ends with a chosen suffix.
#[derive(Debug)]
struct RefusingStore {
    inner: InMemory,
    refused: Mutex<Option<String>>,
}

impl RefusingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            refused: Mutex::new(None),
        }
    }

    fn refuse(&self, suffix: Option<String>) {
        *self.refused.lock().expect("the refusal") = suffix;
    }

    fn check(&self, location: &Path) -> object_store::Result<()> {
        match self.refused.lock().expect("the refusal").as_deref() {
            Some(suffix) if location.as_ref().ends_with(suffix) => {
                Err(object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source: "refused by the test".into(),
                })
            }
            _ => Ok(()),
        }
    }
}

impl std::fmt::Display for RefusingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RefusingStore")
    }
}

#[async_trait]
impl ObjectStore for RefusingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(location)?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(location)?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
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
}

#[tokio::test]
async fn a_failed_publication_leaves_no_index_entry_and_a_retry_completes_it() {
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let binding = format!("{RECORD_PREFIX}/{TENANT}/{FIXTURE_ULID}/import.json");
    let committed = format!("{RECORD_PREFIX}/{TENANT}/by-sha256/{sha256}.json");
    let committed_keys = |indexes: usize| {
        [binding.clone(), committed.clone()]
            .into_iter()
            .chain(record.objects.iter().map(|object| object.block_key.clone()))
            .chain(
                record.objects[..indexes]
                    .iter()
                    .map(|object| object.index_key.clone()),
            )
            .collect::<BTreeSet<_>>()
    };
    // The refused write, and the objects that stay after the failure. The
    // manifests of a committed import stay, but no query reads them before
    // the marker exists.
    let cases = [
        ("/import.json".to_owned(), BTreeSet::new()),
        ("/float.parquet".to_owned(), BTreeSet::new()),
        ("/native-histograms.parquet".to_owned(), BTreeSet::new()),
        (format!("/{sha256}.json"), BTreeSet::new()),
        ("/float.index".to_owned(), committed_keys(0)),
        ("/native-histograms.index".to_owned(), committed_keys(1)),
        ("/_published".to_owned(), committed_keys(2)),
    ];

    for (refused, remaining) in cases {
        let refusing = Arc::new(RefusingStore::new());
        let store: Arc<dyn ObjectStore> = refusing.clone();
        refusing.refuse(Some(refused.clone()));

        let error = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block).await;

        check!(error.is_err(), "refused: {refused}");
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                .is_empty(),
            "refused: {refused}"
        );
        check!(
            all_keys(store.as_ref()).await == remaining,
            "refused: {refused}"
        );

        refusing.refuse(None);
        let retry = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the retry publishes");
        check!(
            retry == TsdbImportOutcome::Imported(record.clone()),
            "refused: {refused}"
        );
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                .len()
                == 2,
            "refused: {refused}"
        );
    }
}

#[tokio::test]
async fn an_import_that_stopped_between_its_manifests_is_finished_by_the_next_import() {
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let committed = Path::from(format!("{RECORD_PREFIX}/{TENANT}/by-sha256/{sha256}.json"));
    let cases = [
        (FIXTURE_ULID, TsdbImportOutcome::Imported(record.clone())),
        (
            OTHER_ULID,
            TsdbImportOutcome::AlreadyImported(record.clone()),
        ),
    ];

    for (ulid, expected) in cases {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the first import publishes");
        // Leave the state of a process that stopped after the float manifest:
        // the record is not published, and the marker, the other block and
        // its manifest are gone.
        let unpublished = TsdbImportRecord {
            published: false,
            ..record.clone()
        };
        store
            .put(
                &committed,
                PutPayload::from(serde_json::to_vec(&unpublished).expect("encode the record")),
            )
            .await
            .expect("seed the record");
        for key in [
            &record.objects[1].block_key,
            &record.objects[1].index_key,
            &marker_key(FIXTURE_ULID, &sha256),
        ] {
            store
                .delete(&Path::from(key.as_str()))
                .await
                .expect("delete the object");
        }
        let hidden = list_compaction_manifests(&store)
            .await
            .expect("list manifests");

        let outcome = publish_tsdb_import(&store, target(ulid, &sha256), &block)
            .await
            .expect("the next import finishes the first");

        check!(outcome == expected, "ULID: {ulid}");
        let manifests = list_compaction_manifests(&store)
            .await
            .expect("list manifests");
        let expected_manifests: Vec<_> = record
            .objects
            .iter()
            .map(|object| expected_manifest(object, &block))
            .collect();
        check!(hidden.is_empty(), "ULID: {ulid}");
        check!(manifests == expected_manifests, "ULID: {ulid}");
    }
}

#[tokio::test]
async fn a_published_import_is_not_restored_after_compaction_removed_its_manifests() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the first import publishes");
    // Compaction merges the imported blocks into other blocks and deletes
    // them with their manifests.
    for object in &record.objects {
        for key in [&object.block_key, &object.index_key] {
            store
                .delete(&Path::from(key.as_str()))
                .await
                .expect("delete the object");
        }
    }

    let outcome = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the repeated import answers");

    check!(outcome == TsdbImportOutcome::AlreadyImported(record));
    check!(
        keys_under(store.as_ref(), MANIFEST_PREFIX).await
            == BTreeSet::from([marker_key(FIXTURE_ULID, &sha256)])
    );
}

#[tokio::test]
async fn an_import_record_from_a_newer_build_stops_the_import_before_it_writes() {
    let (block, sha256) = fixture_block(true);
    let future = serde_json::to_vec(&serde_json::json!({"version": 2, "future": true}))
        .expect("encode the record");
    let binding = format!("{RECORD_PREFIX}/{TENANT}/{FIXTURE_ULID}/import.json");
    let committed = format!("{RECORD_PREFIX}/{TENANT}/by-sha256/{sha256}.json");

    for key in [binding, committed] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(key.as_str()), PutPayload::from(future.clone()))
            .await
            .expect("seed the record");

        let error = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect_err("a newer record stops the import");

        assert!(let TsdbPublishError::UnsupportedRecordVersion { key: found, version: 2, supported: 1 } = error);
        check!(found == key);
        check!(
            keys_under(store.as_ref(), MANIFEST_PREFIX).await.is_empty(),
            "key: {key}"
        );
    }
}

#[tokio::test]
async fn an_invalid_target_is_rejected_before_any_write() {
    let (block, sha256) = fixture_block(true);
    let upper = sha256.to_uppercase();
    let cases = [
        TsdbImportTarget {
            tenant: "tenant-b",
            ..target(FIXTURE_ULID, &sha256)
        },
        target("01m3mjxm7r4m5x4q4ckhw5q8n0", &sha256),
        target("01M3MJXM7R4M5X4Q4CKHW5Q8N", &sha256),
        target(FIXTURE_ULID, &upper),
        target(FIXTURE_ULID, &sha256[1..]),
    ];

    for case in cases {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let error = publish_tsdb_import(&store, case, &block).await;

        check!(let Err(TsdbPublishError::InvalidTarget(_)) = error, "target: {case:?}");
        check!(
            all_keys(store.as_ref()).await.is_empty(),
            "target: {case:?}"
        );
    }
}

/// One retention window for every tenant.
struct Windows(Time);

impl krabka_blockstore::RetentionWindows for Windows {
    fn block_retention(&self, _tenant: &str) -> Time {
        self.0
    }
}

/// A time after the orphan sweep grace of every object that a test writes
/// now.
fn after_the_sweep_grace() -> SystemTime {
    SystemTime::now() + Duration::from_hours(2)
}

async fn missing_blocks(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let index = list_compaction_index(store).await.expect("list the index");
    let mut missing = Vec::new();
    for manifest in index.live.iter().chain(&index.pending) {
        if store
            .head(&Path::from(manifest.block_key.as_str()))
            .await
            .is_err()
        {
            missing.push(manifest.block_key.clone());
        }
    }
    missing
}

#[tokio::test]
async fn an_import_that_stops_at_any_write_is_live_whole_or_not_at_all() {
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let all_manifests: Vec<_> = record
        .objects
        .iter()
        .map(|object| expected_manifest(object, &block))
        .collect();
    // The write where the process stops, and whether the import is live after
    // the stop.
    let cases = [
        ("/import.json", 1, false),
        ("/float.parquet", 1, false),
        ("/native-histograms.parquet", 1, false),
        ("/by-sha256/", 1, false),
        ("/float.index", 1, false),
        ("/native-histograms.index", 1, false),
        ("/_published", 1, false),
        ("/by-sha256/", 2, true),
    ];

    for (key_part, occurrence, live) in cases {
        let crashing = Arc::new(CrashingStore::default());
        let store: Arc<dyn ObjectStore> = crashing.clone();
        crashing.crash_at(Some(CrashPoint {
            key_part: key_part.to_owned(),
            occurrence,
        }));

        let stopped = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block).await;

        let case = format!("{key_part} #{occurrence}");
        check!(crashing.crashed(), "case: {case}");
        check!(stopped.is_err(), "case: {case}");
        let expected = if live {
            all_manifests.clone()
        } else {
            Vec::new()
        };
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                == expected,
            "case: {case}"
        );

        // Another process sweeps orphans after the grace period, then a retry
        // finishes the import.
        crashing.crash_at(None);
        enforce_compaction_retention(&store, after_the_sweep_grace(), &Windows(secs(0)))
            .await
            .expect("sweep orphans");
        check!(missing_blocks(&store).await.is_empty(), "case: {case}");
        let retry = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the retry publishes");

        check!(
            retry == TsdbImportOutcome::Imported(record.clone()),
            "case: {case}"
        );
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                == all_manifests,
            "case: {case}"
        );
        check!(missing_blocks(&store).await.is_empty(), "case: {case}");
    }
}

#[tokio::test]
async fn retention_and_the_orphan_sweep_keep_an_unpublished_import() {
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let crashing = Arc::new(CrashingStore::default());
    let store: Arc<dyn ObjectStore> = crashing.clone();
    crashing.crash_at(Some(CrashPoint {
        key_part: "/_published".to_owned(),
        occurrence: 1,
    }));
    publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect_err("the process stops before the marker");
    crashing.crash_at(None);
    let before = keys_under(store.as_ref(), MANIFEST_PREFIX).await;

    // A one-second window expires every live block of the fixture.
    let stats = enforce_compaction_retention(&store, after_the_sweep_grace(), &Windows(secs(1)))
        .await
        .expect("enforce retention");

    check!(stats.manifests_scanned == 0);
    check!(stats.orphans.deleted == 0);
    check!(keys_under(store.as_ref(), MANIFEST_PREFIX).await == before);
    let pending = list_compaction_index(&store)
        .await
        .expect("list the index")
        .pending;
    let expected: Vec<_> = record
        .objects
        .iter()
        .map(|object| expected_manifest(object, &block))
        .collect();
    check!(pending == expected);
}

#[tokio::test]
async fn an_import_that_stopped_after_its_marker_is_not_published_again() {
    let (block, sha256) = fixture_block(true);
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let crashing = Arc::new(CrashingStore::default());
    let store: Arc<dyn ObjectStore> = crashing.clone();
    crashing.crash_at(Some(CrashPoint {
        key_part: "/by-sha256/".to_owned(),
        occurrence: 2,
    }));
    publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect_err("the process stops before the record is published");
    crashing.crash_at(None);
    // Compaction merges the live blocks into other blocks and deletes them
    // with their manifests.
    for object in &record.objects {
        for key in [&object.block_key, &object.index_key] {
            store
                .delete(&Path::from(key.as_str()))
                .await
                .expect("delete the object");
        }
    }

    let outcome = publish_tsdb_import(&store, target(OTHER_ULID, &sha256), &block)
        .await
        .expect("the next import marks the record published");

    check!(outcome == TsdbImportOutcome::AlreadyImported(record));
    check!(
        keys_under(store.as_ref(), MANIFEST_PREFIX).await
            == BTreeSet::from([marker_key(FIXTURE_ULID, &sha256)])
    );
}

#[test]
fn a_listing_hides_the_manifests_of_an_import_directory_without_a_marker() {
    const PUBLISHED: &str = "metrics/tenant-a/uploaded/01M3MJXM7R4M5X4Q4CKHW5Q8N0-62593eb43039a3ec";
    const PENDING: &str = "metrics/tenant-a/uploaded/01M3MJXM7R4M5X4Q4CKHW5Q8N1-62593eb43039a3ec";
    let keys = [
        format!("{PUBLISHED}/_published"),
        format!("{PUBLISHED}/float.index"),
        format!("{PUBLISHED}/float.parquet"),
        format!("{PUBLISHED}/native-histograms.INDEX"),
        format!("{PENDING}/float.index"),
        format!("{PENDING}/native-histograms.index"),
        // Not import directories: a native block upload, a block-builder
        // block, and directories whose name is not `<ULID>-<16 hex digits>`.
        "metrics/tenant-a/uploaded/01M3MJXM7R4M5X4Q4CKHW5Q8N2.index".to_owned(),
        "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index".to_owned(),
        "metrics/tenant-a/uploaded/01m3mjxm7r4m5x4q4ckhw5q8n0-62593eb43039a3ec/float.index"
            .to_owned(),
        "metrics/tenant-a/uploaded/01M3MJXM7R4M5X4Q4CKHW5Q8N0-62593EB43039A3EC/float.index"
            .to_owned(),
        "metrics/tenant-a/other/01M3MJXM7R4M5X4Q4CKHW5Q8N0-62593eb43039a3ec/float.index".to_owned(),
        // A marker outside an import directory is not a marker.
        "metrics/tenant-a/float/_published".to_owned(),
    ];

    let listing = CompactionIndexListing::new(keys.to_vec(), String::as_str);

    check!(
        listing
            == CompactionIndexListing {
                live: vec![
                    format!("{PUBLISHED}/float.index"),
                    format!("{PUBLISHED}/native-histograms.INDEX"),
                    keys[6].clone(),
                    keys[7].clone(),
                    keys[8].clone(),
                    keys[9].clone(),
                    keys[10].clone(),
                ],
                pending: vec![
                    format!("{PENDING}/float.index"),
                    format!("{PENDING}/native-histograms.index"),
                ],
                markers: vec![format!("{PUBLISHED}/_published")],
            }
    );
}

#[test]
fn the_tenant_keys_name_live_and_pending_objects_and_markers_of_one_tenant() {
    let manifest = |tenant: &str, stem: &str| CompactionIndexManifest {
        tenant: tenant.to_owned(),
        kind: MetricBlockKind::Float,
        block_key: format!("{stem}.parquet"),
        index_key: format!("{stem}.index"),
        level: BlockLevel::INGESTED,
        first_offset: 0,
        last_offset: 0,
        row_count: 1,
        min_ts: 0,
        max_ts: 0,
        fingerprints: Vec::new(),
        series: Vec::new(),
    };
    let index = CompactionIndex {
        live: vec![
            manifest(TENANT, "metrics/tenant-a/float/live"),
            manifest("tenant-b", "metrics/tenant-b/float/live"),
        ],
        pending: vec![manifest(TENANT, "metrics/tenant-a/uploaded/pending/float")],
        markers: vec![
            "metrics/tenant-a/uploaded/published/_published".to_owned(),
            "metrics/tenant-a-2/uploaded/published/_published".to_owned(),
        ],
    };

    let keys = index.tenant_keys(TENANT);

    check!(
        keys == [
            "metrics/tenant-a/float/live.index",
            "metrics/tenant-a/float/live.parquet",
            "metrics/tenant-a/uploaded/pending/float.index",
            "metrics/tenant-a/uploaded/pending/float.parquet",
            "metrics/tenant-a/uploaded/published/_published",
        ]
    );
}

/// The storage audit reads the store that an import writes. A published
/// import must audit clean, and the repair must find nothing to delete in an
/// import that stopped before it published: a retry still needs those objects.
#[tokio::test]
async fn the_storage_audit_reports_no_damage_or_orphan_in_an_import() {
    use krabka_blockstore::{StorageAuditOptions, StorageSignal, audit_store};

    let (block, sha256) = fixture_block(true);
    let audit = |store: Arc<dyn ObjectStore>| async move {
        let mut options = StorageAuditOptions::new(SystemTime::now() + Duration::from_hours(2));
        options.signal = Some(StorageSignal::Metrics);
        audit_store(&store, &options).await.expect("the audit runs")
    };

    let published: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    publish_tsdb_import(&published, target(FIXTURE_ULID, &sha256), &block)
        .await
        .expect("the import publishes");
    let report = audit(published).await;
    check!(
        report.findings == Vec::new(),
        "published: {:#?}",
        report.findings
    );

    let crashing = Arc::new(CrashingStore::default());
    let stopped: Arc<dyn ObjectStore> = crashing.clone();
    crashing.crash_at(Some(CrashPoint {
        key_part: "/_published".to_owned(),
        occurrence: 1,
    }));
    check!(
        publish_tsdb_import(&stopped, target(FIXTURE_ULID, &sha256), &block)
            .await
            .is_err()
    );
    crashing.crash_at(None);
    let report = audit(stopped).await;
    check!(
        report.findings.iter().all(|finding| !finding.repairable),
        "stopped: {:#?}",
        report.findings
    );
}
