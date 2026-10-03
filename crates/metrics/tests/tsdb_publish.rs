use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use async_trait::async_trait;
use futures::{TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::BlockLevel;
use krabka_metrics::{
    CompactionIndexManifest, CompactionSeriesLabels, DecodedTsdbBlock, MetricBlockKind,
    TsdbBlockFiles, TsdbBlockMeta, TsdbImportLimits, TsdbImportObject, TsdbImportOutcome,
    TsdbImportRecord, TsdbImportTarget, TsdbPublishError, decode_float_samples,
    decode_native_histograms, decode_tsdb_block, list_compaction_manifests, publish_tsdb_import,
    tsdb_block_sha256,
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tsdb_fixture::{FIXTURE_MAX_TIME, FIXTURE_MIN_TIME, FIXTURE_ULID, fixture_files};

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

fn object_keys(ulid: &str, sha256: &str, kind: &str) -> (String, String) {
    let stem = format!("metrics/{TENANT}/uploaded/{ulid}-{}/{kind}", &sha256[..16]);
    (format!("{stem}.parquet"), format!("{stem}.index"))
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
    let blocks = [
        record.objects[0].block_key.clone(),
        record.objects[1].block_key.clone(),
    ];
    // The refused write, and the objects that stay after the failure.
    let cases = [
        ("/import.json".to_owned(), BTreeSet::new()),
        ("/float.parquet".to_owned(), BTreeSet::new()),
        ("/native-histograms.parquet".to_owned(), BTreeSet::new()),
        (format!("/{sha256}.json"), BTreeSet::new()),
        (
            "/native-histograms.index".to_owned(),
            [binding, committed]
                .into_iter()
                .chain(blocks)
                .collect::<BTreeSet<_>>(),
        ),
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
        // the record is not published, and the other block and its manifest
        // are gone.
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
        for key in [&record.objects[1].block_key, &record.objects[1].index_key] {
            store
                .delete(&Path::from(key.as_str()))
                .await
                .expect("delete the object");
        }

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
    check!(keys_under(store.as_ref(), MANIFEST_PREFIX).await.is_empty());
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
