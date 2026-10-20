#![allow(clippy::items_after_statements)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use assert2::{assert, check};
use futures::TryStreamExt as _;
use krabka_blockstore::BlockLevel;
use krabka_metrics::{
    CompactionIndex, CompactionIndexListing, CompactionIndexManifest, CompactionSeriesLabels,
    DecodedTsdbBlock, MetricBlockKind, NativeHistogramRow, TsdbBlockFiles, TsdbBlockMeta,
    TsdbImportLimits, TsdbImportObject, TsdbImportOutcome, TsdbImportRecord, TsdbImportTarget,
    TsdbPublishError, decode_float_samples, decode_native_histograms, decode_tsdb_block,
    enforce_compaction_retention, list_compaction_index, list_compaction_manifests,
    publish_tsdb_import, tsdb_block_sha256,
};
use krabka_units::{Time, secs};
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, memory::InMemory, path::Path,
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

struct RefusalCase {
    refused: String,
    remaining: BTreeSet<String>,
}
struct OutcomeCase {
    ulid: &'static str,
    expected: TsdbImportOutcome,
}
struct ImportCrashCase {
    key_part: &'static str,
    occurrence: usize,
    live: bool,
}

/// The fixture decoded with or without its tombstones, and its content hash.
/// The fixture import into a fresh store, stopped by a crash at one write
/// and left as that crash left it.
struct InterruptedImport {
    store: Arc<dyn ObjectStore>,
    block: DecodedTsdbBlock,
    sha256: String,
    /// The record the import would have published.
    record: TsdbImportRecord,
}

impl InterruptedImport {
    async fn stop_at(crash_point: CrashPoint) -> Self {
        let fixture = fixture_block(true);
        let block = fixture.block;
        let sha256 = fixture.sha256;
        let record = expected_record(FIXTURE_ULID, &sha256, &block);
        let crashing = Arc::new(CrashingStore::default());
        let store: Arc<dyn ObjectStore> = crashing.clone();
        crashing.crash_at(Some(crash_point));
        publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect_err("the process stops at the crash point");
        crashing.crash_at(None);
        Self {
            store,
            block,
            sha256,
            record,
        }
    }
}

/// A store that holds the tombstoned fixture block, published under
/// [`FIXTURE_ULID`].
struct PublishedFixture {
    store: Arc<dyn ObjectStore>,
    block: DecodedTsdbBlock,
    sha256: String,
}

impl PublishedFixture {
    async fn publish() -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fixture = fixture_block(true);
        let block = fixture.block;
        let sha256 = fixture.sha256;
        publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the first import publishes");
        Self {
            store,
            block,
            sha256,
        }
    }
}

struct FixtureBlock {
    block: DecodedTsdbBlock,
    sha256: String,
}

fn fixture_block(with_tombstones: bool) -> FixtureBlock {
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
    FixtureBlock {
        block,
        sha256: tsdb_block_sha256(block_files),
    }
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

async fn delete_import_objects(store: &dyn ObjectStore, record: &TsdbImportRecord) {
    for object in &record.objects {
        for key in [&object.block_key, &object.index_key] {
            store
                .delete(&Path::from(key.as_str()))
                .await
                .expect("delete the object");
        }
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

struct TestObjectKeys {
    block_key: String,
    index_key: String,
}

fn object_keys(ulid: &str, sha256: &str, kind: &str) -> TestObjectKeys {
    let stem = format!("{}/{kind}", import_directory(ulid, sha256));
    TestObjectKeys {
        block_key: format!("{stem}.parquet"),
        index_key: format!("{stem}.index"),
    }
}

fn marker_key(ulid: &str, sha256: &str) -> String {
    format!("{}/_published", import_directory(ulid, sha256))
}

fn expected_record(ulid: &str, sha256: &str, block: &DecodedTsdbBlock) -> TsdbImportRecord {
    let float_keys = object_keys(ulid, sha256, "float");
    let histogram_keys = object_keys(ulid, sha256, "native-histograms");
    TsdbImportRecord {
        version: 1,
        ulid: ulid.to_owned(),
        sha256: sha256.to_owned(),
        stats: block.stats,
        objects: vec![
            TsdbImportObject {
                kind: MetricBlockKind::Float,
                block_key: float_keys.block_key,
                index_key: float_keys.index_key,
                rows: u64::try_from(block.rows.float_rows.len()).expect("row count"),
            },
            TsdbImportObject {
                kind: MetricBlockKind::NativeHistograms,
                block_key: histogram_keys.block_key,
                index_key: histogram_keys.index_key,
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
    struct SampleKey {
        fingerprint: u64,
        timestamp_ms: i64,
    }

    let samples: Vec<SampleKey> = match object.kind {
        MetricBlockKind::Float => block
            .rows
            .float_rows
            .iter()
            .map(|row| SampleKey {
                fingerprint: row.fingerprint,
                timestamp_ms: row.timestamp_ms,
            })
            .collect(),
        _ => block
            .rows
            .histogram_rows
            .iter()
            .map(|row| SampleKey {
                fingerprint: row.fingerprint,
                timestamp_ms: row.timestamp_ms,
            })
            .collect(),
    };
    let fingerprints: BTreeSet<u64> = samples.iter().map(|sample| sample.fingerprint).collect();
    CompactionIndexManifest {
        tenant: TENANT.to_owned(),
        kind: object.kind,
        block_key: object.block_key.clone(),
        index_key: object.index_key.clone(),
        level: BlockLevel::INGESTED,
        first_offset: 0,
        last_offset: 0,
        row_count: samples.len(),
        min_ts: samples
            .iter()
            .map(|sample| sample.timestamp_ms)
            .min()
            .expect("samples"),
        max_ts: samples
            .iter()
            .map(|sample| sample.timestamp_ms)
            .max()
            .expect("samples"),
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
    let fixture = fixture_block(true);
    let sha256 = fixture.sha256;

    check!(sha256 == FIXTURE_SHA256);
}

#[test]
fn the_content_hash_changes_when_bytes_move_between_files() {
    let files = fixture_files();
    let whole = [files.chunks.as_slice()];
    let split_chunks = files.chunks.split_at(files.chunks.len() / 2);
    let split = [split_chunks.0, split_chunks.1];
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
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;

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
    #[derive(PartialEq)]
    struct FloatRowSnapshot {
        fingerprint: u64,
        timestamp_ms: i64,
        value_bits: u64,
        start_timestamp_ms: Option<i64>,
    }
    let floats: Vec<_> = float_batches
        .iter()
        .flat_map(|batch| decode_float_samples(batch).expect("decode floats"))
        .map(|row| FloatRowSnapshot {
            fingerprint: row.fingerprint,
            timestamp_ms: row.timestamp_ms,
            value_bits: row.value.to_bits(),
            start_timestamp_ms: row.start_timestamp_ms,
        })
        .collect();
    let expected_floats: Vec<_> = block
        .rows
        .float_rows
        .iter()
        .map(|row| FloatRowSnapshot {
            fingerprint: row.fingerprint,
            timestamp_ms: row.timestamp_ms,
            value_bits: row.value.to_bits(),
            start_timestamp_ms: row.start_timestamp_ms,
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
        .map(|row| NativeHistogramRow {
            fingerprint: row.fingerprint,
            timestamp_ms: row.timestamp_ms,
            hist: row.hist.clone(),
        })
        .collect();
    check!(histograms == expected_histograms);
}

#[tokio::test]
async fn a_repeated_import_reports_the_existing_content_and_writes_nothing() {
    let PublishedFixture {
        store,
        block,
        sha256,
    } = PublishedFixture::publish().await;
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
    let PublishedFixture {
        store,
        block: _,
        sha256,
    } = PublishedFixture::publish().await;
    let before = all_keys(store.as_ref()).await;
    let other_fixture = fixture_block(false);
    let other_block = other_fixture.block;
    let other_sha256 = other_fixture.sha256;

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

krabka_blockstore::delegate_object_store! {
    RefusingStore => inner;
    forward [get_opts, delete_stream, list, list_with_delimiter, copy_opts];

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
}

#[tokio::test]
async fn a_failed_publication_leaves_no_index_entry_and_a_retry_completes_it() {
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
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
        RefusalCase {
            refused: "/import.json".to_owned(),
            remaining: BTreeSet::new(),
        },
        RefusalCase {
            refused: "/float.parquet".to_owned(),
            remaining: BTreeSet::new(),
        },
        RefusalCase {
            refused: "/native-histograms.parquet".to_owned(),
            remaining: BTreeSet::new(),
        },
        RefusalCase {
            refused: format!("/{sha256}.json"),
            remaining: BTreeSet::new(),
        },
        RefusalCase {
            refused: "/float.index".to_owned(),
            remaining: committed_keys(0),
        },
        RefusalCase {
            refused: "/native-histograms.index".to_owned(),
            remaining: committed_keys(1),
        },
        RefusalCase {
            refused: "/_published".to_owned(),
            remaining: committed_keys(2),
        },
    ];

    for case in cases {
        let refusing = Arc::new(RefusingStore::new());
        let store: Arc<dyn ObjectStore> = refusing.clone();
        refusing.refuse(Some(case.refused.clone()));

        let error = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block).await;

        check!(error.is_err(), "refused: {}", case.refused);
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                .is_empty(),
            "refused: {}",
            case.refused
        );
        check!(
            all_keys(store.as_ref()).await == case.remaining,
            "refused: {}",
            case.refused
        );

        refusing.refuse(None);
        let retry = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the retry publishes");
        check!(
            retry == TsdbImportOutcome::Imported(record.clone()),
            "refused: {}",
            case.refused
        );
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                .len()
                == 2,
            "refused: {}",
            case.refused
        );
    }
}

#[tokio::test]
async fn an_import_that_stopped_between_its_manifests_is_finished_by_the_next_import() {
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let committed = Path::from(format!("{RECORD_PREFIX}/{TENANT}/by-sha256/{sha256}.json"));
    let cases = [
        OutcomeCase {
            ulid: FIXTURE_ULID,
            expected: TsdbImportOutcome::Imported(record.clone()),
        },
        OutcomeCase {
            ulid: OTHER_ULID,
            expected: TsdbImportOutcome::AlreadyImported(record.clone()),
        },
    ];

    for case in cases {
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

        let outcome = publish_tsdb_import(&store, target(case.ulid, &sha256), &block)
            .await
            .expect("the next import finishes the first");

        check!(outcome == case.expected, "ULID: {}", case.ulid);
        let manifests = list_compaction_manifests(&store)
            .await
            .expect("list manifests");
        let expected_manifests: Vec<_> = record
            .objects
            .iter()
            .map(|object| expected_manifest(object, &block))
            .collect();
        check!(hidden.is_empty(), "ULID: {}", case.ulid);
        check!(manifests == expected_manifests, "ULID: {}", case.ulid);
    }
}

#[tokio::test]
async fn a_published_import_is_not_restored_after_compaction_removed_its_manifests() {
    let PublishedFixture {
        store,
        block,
        sha256,
    } = PublishedFixture::publish().await;
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    // Compaction merges the imported blocks into other blocks and deletes
    // them with their manifests.
    delete_import_objects(store.as_ref(), &record).await;

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
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
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
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
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
    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
    let record = expected_record(FIXTURE_ULID, &sha256, &block);
    let all_manifests: Vec<_> = record
        .objects
        .iter()
        .map(|object| expected_manifest(object, &block))
        .collect();
    // The write where the process stops, and whether the import is live after
    // the stop.
    let cases = [
        ImportCrashCase {
            key_part: "/import.json",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/float.parquet",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/native-histograms.parquet",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/by-sha256/",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/float.index",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/native-histograms.index",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/_published",
            occurrence: 1,
            live: false,
        },
        ImportCrashCase {
            key_part: "/by-sha256/",
            occurrence: 2,
            live: true,
        },
    ];

    for case in cases {
        let crashing = Arc::new(CrashingStore::default());
        let store: Arc<dyn ObjectStore> = crashing.clone();
        crashing.crash_at(Some(CrashPoint {
            key_part: case.key_part.to_owned(),
            occurrence: case.occurrence,
        }));

        let stopped = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block).await;

        let case_name = format!("{} #{}", case.key_part, case.occurrence);
        check!(crashing.crashed(), "case: {case_name}");
        check!(stopped.is_err(), "case: {case_name}");
        let expected = if case.live {
            all_manifests.clone()
        } else {
            Vec::new()
        };
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                == expected,
            "case: {case_name}"
        );

        // Another process sweeps orphans after the grace period, then a retry
        // finishes the import.
        crashing.crash_at(None);
        enforce_compaction_retention(&store, after_the_sweep_grace(), &Windows(secs(0)))
            .await
            .expect("sweep orphans");
        check!(missing_blocks(&store).await.is_empty(), "case: {case_name}");
        let retry = publish_tsdb_import(&store, target(FIXTURE_ULID, &sha256), &block)
            .await
            .expect("the retry publishes");

        check!(
            retry == TsdbImportOutcome::Imported(record.clone()),
            "case: {case_name}"
        );
        check!(
            list_compaction_manifests(&store)
                .await
                .expect("list manifests")
                == all_manifests,
            "case: {case_name}"
        );
        check!(missing_blocks(&store).await.is_empty(), "case: {case_name}");
    }
}

#[tokio::test]
async fn retention_and_the_orphan_sweep_keep_an_unpublished_import() {
    let InterruptedImport {
        store,
        block,
        sha256: _,
        record,
    } = InterruptedImport::stop_at(CrashPoint {
        key_part: "/_published".to_owned(),
        occurrence: 1,
    })
    .await;
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
    let InterruptedImport {
        store,
        block,
        sha256,
        record,
    } = InterruptedImport::stop_at(CrashPoint {
        key_part: "/by-sha256/".to_owned(),
        occurrence: 2,
    })
    .await;
    // Compaction merges the live blocks into other blocks and deletes them
    // with their manifests.
    delete_import_objects(store.as_ref(), &record).await;

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

    let fixture = fixture_block(true);
    let block = fixture.block;
    let sha256 = fixture.sha256;
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
