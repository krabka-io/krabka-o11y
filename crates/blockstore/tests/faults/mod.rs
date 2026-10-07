//! Provider faults injected on the client side, and the cases that prove
//! Krabka's response to each one.
//!
//! Each wrapper decorates any [`ObjectStore`], so the same case runs over an
//! in-memory store in `object_store_faults` and over a real provider in
//! `object_store_provider_contract`. A real provider seldom throttles, corrupts
//! or lags on demand. A wrapper makes the fault happen every time.
//!
//! The three faults have the shapes the `object_store` HTTP clients produce:
//!
//! - A throttled request (S3 `503 SlowDown`, GCS and Azure `429` or `503`)
//!   that outlasts the client's own retries arrives as
//!   [`ObjectStoreError::Generic`].
//! - An upload whose checksum the provider rejects (S3 `400 BadDigest`)
//!   arrives as [`ObjectStoreError::Generic`] too. Only 401, 403, 404, 409
//!   and 412 have variants of their own.
//! - A stale listing omits an object that a completed put wrote.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use arrow::{
    array::{Int64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP, ObjectStoreMetrics, ObjectStoreOperation,
    ObjectStoreRetryPolicy, OrphanSweepStats, RetryingObjectStore, read_block, reconcile_orphans,
    transient_object_store_error,
};
use krabka_units::{Time, convert::TimeExt as _};
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, path::Path,
};

/// The attempts every case gives the retry layer.
///
/// It matches [`ObjectStoreRetryPolicy::DEFAULT`], without the waiting.
const ATTEMPTS: u32 = 4;

fn throttled() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "S3",
        source: "Server returned non-2xx status code: 503 Service Unavailable: SlowDown".into(),
    }
}

fn bad_digest() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "S3",
        source: "Server returned non-2xx status code: 400 Bad Request: BadDigest: \
                 The checksum you specified did not match what we received"
            .into(),
    }
}

/// Names an error by its variant, so an outcome compares as plain data.
fn kind(error: &ObjectStoreError) -> &'static str {
    match error {
        ObjectStoreError::Generic { .. } => "Generic",
        ObjectStoreError::NotFound { .. } => "NotFound",
        ObjectStoreError::Precondition { .. } => "Precondition",
        ObjectStoreError::AlreadyExists { .. } => "AlreadyExists",
        ObjectStoreError::PermissionDenied { .. } => "PermissionDenied",
        _ => "other",
    }
}

/// Delegates every operation to `inner`. Each wrapper below overrides only
/// the operations its fault touches.
macro_rules! delegate_object_store {
    ($wrapper:ident { $($overrides:tt)* }) => {
        impl std::fmt::Display for $wrapper {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}({})", stringify!($wrapper), self.inner)
            }
        }

        #[async_trait]
        impl ObjectStore for $wrapper {
            $($overrides)*

            async fn put_multipart_opts(
                &self,
                location: &Path,
                options: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, options).await
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&Path>,
            ) -> object_store::Result<ListResult> {
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
                self.inner.delete_stream(locations)
            }
        }
    };
}

/// Refuses the first `failures` puts as throttled, then delegates.
#[derive(Debug)]
struct ThrottlingStore {
    inner: Arc<dyn ObjectStore>,
    remaining: AtomicUsize,
    attempts: AtomicUsize,
}

impl ThrottlingStore {
    fn new(inner: Arc<dyn ObjectStore>, failures: usize) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(failures),
            attempts: AtomicUsize::new(0),
        }
    }
}

delegate_object_store!(ThrottlingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let refused = self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if refused {
            return Err(throttled());
        }
        self.inner.put_opts(location, payload, options).await
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
});

/// How a [`ChecksumStore`] breaks the bytes it carries.
#[derive(Clone, Copy, Debug)]
enum ChecksumFault {
    /// The provider rejects every upload as `BadDigest` and stores nothing.
    RejectPuts,
    /// The bytes change in transit and no transport check notices. The last
    /// byte of every read that reaches the end of the object is flipped.
    CorruptReads,
}

#[derive(Debug)]
struct ChecksumStore {
    inner: Arc<dyn ObjectStore>,
    fault: ChecksumFault,
    put_attempts: AtomicUsize,
}

impl ChecksumStore {
    fn new(inner: Arc<dyn ObjectStore>, fault: ChecksumFault) -> Self {
        Self {
            inner,
            fault,
            put_attempts: AtomicUsize::new(0),
        }
    }
}

delegate_object_store!(ChecksumStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, ChecksumFault::RejectPuts) {
            return Err(bad_digest());
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        if !matches!(self.fault, ChecksumFault::CorruptReads) {
            return Ok(result);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let mut bytes = result.bytes().await?.to_vec();
        if range.end == meta.size
            && let Some(last) = bytes.last_mut()
        {
            *last ^= 0xFF;
        }
        let payload = futures::stream::once(async move { Ok(Bytes::from(bytes)) }).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
});

/// Omits each put from the next `lag` listings, as an eventually consistent
/// listing does.
#[derive(Debug)]
struct StaleListingStore {
    inner: Arc<dyn ObjectStore>,
    lag: usize,
    hidden: Arc<Mutex<BTreeMap<Path, usize>>>,
}

impl StaleListingStore {
    fn new(inner: Arc<dyn ObjectStore>, lag: usize) -> Self {
        Self {
            inner,
            lag,
            hidden: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

delegate_object_store!(StaleListingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(location, payload, options).await?;
        self.hidden
            .lock()
            .expect("the hidden set is not poisoned")
            .insert(location.clone(), self.lag);
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let omitted: BTreeSet<Path> = {
            let mut hidden = self.hidden.lock().expect("the hidden set is not poisoned");
            let omitted = hidden.keys().cloned().collect();
            hidden.retain(|_, left| {
                *left -= 1;
                *left > 0
            });
            omitted
        };
        self.inner
            .list(prefix)
            .try_filter(move |meta| futures::future::ready(!omitted.contains(&meta.location)))
            .boxed()
    }
});

/// What the retry layer made of a throttled put.
#[derive(Debug, PartialEq, Eq)]
pub struct ThrottlingOutcome {
    /// Throttled on all but the last attempt: the read-back payload.
    pub within_budget: Result<Vec<u8>, &'static str>,
    pub within_budget_attempts: usize,
    pub within_budget_retries: u64,
    /// Throttled on every attempt: the error the caller sees.
    pub beyond_budget: Result<(), &'static str>,
    pub beyond_budget_attempts: usize,
    /// Whether the refused put left an object behind.
    pub beyond_budget_object: bool,
}

impl ThrottlingOutcome {
    /// The outcome that proves the retry layer absorbs throttling.
    pub fn absorbed() -> Self {
        Self {
            within_budget: Ok(b"throttled payload".to_vec()),
            within_budget_attempts: ATTEMPTS as usize,
            within_budget_retries: u64::from(ATTEMPTS - 1),
            beyond_budget: Err("Generic"),
            beyond_budget_attempts: ATTEMPTS as usize,
            beyond_budget_object: false,
        }
    }
}

async fn exists(store: &dyn ObjectStore, location: &Path) -> bool {
    match store.head(location).await {
        Ok(_) => true,
        Err(ObjectStoreError::NotFound { .. }) => false,
        Err(error) => panic!("head {location} failed: {error}"),
    }
}

/// Throttles puts to `base` and reports what the retry layer did.
pub async fn throttling(base: Arc<dyn ObjectStore>) -> ThrottlingOutcome {
    let failures = ATTEMPTS as usize - 1;
    let flaky = Arc::new(ThrottlingStore::new(Arc::clone(&base), failures));
    let metrics = ObjectStoreMetrics::unregistered();
    let store = RetryingObjectStore::wrap(
        Arc::clone(&flaky) as Arc<dyn ObjectStore>,
        ObjectStoreRetryPolicy::immediate(ATTEMPTS),
        metrics.clone(),
    );
    let within = Path::from("faults/throttling/within-budget");
    let within_budget = match store.put(&within, "throttled payload".into()).await {
        Ok(_) => Ok(base
            .get(&within)
            .await
            .expect("the absorbed put is readable")
            .bytes()
            .await
            .expect("the absorbed put reads")
            .to_vec()),
        Err(error) => Err(kind(&error)),
    };

    let refusing = Arc::new(ThrottlingStore::new(Arc::clone(&base), ATTEMPTS as usize));
    let store = RetryingObjectStore::wrap(
        Arc::clone(&refusing) as Arc<dyn ObjectStore>,
        ObjectStoreRetryPolicy::immediate(ATTEMPTS),
        ObjectStoreMetrics::unregistered(),
    );
    let beyond = Path::from("faults/throttling/beyond-budget");
    let beyond_budget = store
        .put(&beyond, "never stored".into())
        .await
        .map(|_| ())
        .map_err(|error| kind(&error));

    ThrottlingOutcome {
        within_budget,
        within_budget_attempts: flaky.attempts.load(Ordering::SeqCst),
        within_budget_retries: metrics.retries(ObjectStoreOperation::Put),
        beyond_budget,
        beyond_budget_attempts: refusing.attempts.load(Ordering::SeqCst),
        beyond_budget_object: exists(base.as_ref(), &beyond).await,
    }
}

/// What Krabka made of bytes whose checksum does not match.
#[derive(Debug, PartialEq, Eq)]
pub struct ChecksumOutcome {
    /// The error an upload the provider rejects ends in.
    pub rejected_upload: Result<(), &'static str>,
    /// How many times the upload was sent. The payload in memory is intact,
    /// so a resend is how a transit corruption clears.
    pub rejected_upload_attempts: usize,
    /// Whether the rejected upload published an object.
    pub rejected_upload_object: bool,
    /// Whether a block read of corrupted bytes failed.
    pub corrupted_read_failed: bool,
    /// Whether the retry layer judged that failure worth another attempt.
    pub corrupted_read_transient: bool,
    /// Read retries the corrupted block cost.
    pub corrupted_read_retries: u64,
}

impl ChecksumOutcome {
    /// The outcome that proves a checksum mismatch never becomes data.
    pub fn surfaced() -> Self {
        Self {
            rejected_upload: Err("Generic"),
            rejected_upload_attempts: ATTEMPTS as usize,
            rejected_upload_object: false,
            corrupted_read_failed: true,
            corrupted_read_transient: false,
            corrupted_read_retries: 0,
        }
    }
}

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]))
}

fn series_batch() -> RecordBatch {
    RecordBatch::try_new(
        series_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![7_u64, 7])),
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .expect("the batch matches its schema")
}

/// Breaks checksums on the way to and from `base`.
pub async fn checksum_mismatch(base: Arc<dyn ObjectStore>) -> ChecksumOutcome {
    let rejecting = Arc::new(ChecksumStore::new(
        Arc::clone(&base),
        ChecksumFault::RejectPuts,
    ));
    let store = RetryingObjectStore::wrap(
        Arc::clone(&rejecting) as Arc<dyn ObjectStore>,
        ObjectStoreRetryPolicy::immediate(ATTEMPTS),
        ObjectStoreMetrics::unregistered(),
    );
    let rejected = Path::from("faults/checksum/rejected");
    let rejected_upload = store
        .put(&rejected, "sent intact, received corrupt".into())
        .await
        .map(|_| ())
        .map_err(|error| kind(&error));

    let key = "faults/checksum/block.parquet";
    BlockWriter::new(Arc::clone(&base))
        .write_block("tenant", key, series_schema(), &[series_batch()])
        .await
        .expect("the intact block writes");
    let metrics = ObjectStoreMetrics::unregistered();
    let corrupting: Arc<dyn ObjectStore> = Arc::new(ChecksumStore::new(
        Arc::clone(&base),
        ChecksumFault::CorruptReads,
    ));
    let store = RetryingObjectStore::wrap(
        corrupting,
        ObjectStoreRetryPolicy::immediate(ATTEMPTS),
        metrics.clone(),
    );
    let read = read_block(store, key).await;

    ChecksumOutcome {
        rejected_upload,
        rejected_upload_attempts: rejecting.put_attempts.load(Ordering::SeqCst),
        rejected_upload_object: exists(base.as_ref(), &rejected).await,
        corrupted_read_failed: read.is_err(),
        corrupted_read_transient: read
            .as_ref()
            .err()
            .is_some_and(|error| transient_object_store_error(error).is_some()),
        corrupted_read_retries: metrics.retries(ObjectStoreOperation::Get),
    }
}

/// What the orphan sweep did while the listing lagged.
#[derive(Debug, PartialEq, Eq)]
pub struct StaleListingOutcome {
    /// The sweep while two puts are still missing from the listing.
    pub while_stale: OrphanSweepStats,
    /// The sweep once the listing has caught up.
    pub once_consistent: OrphanSweepStats,
    /// The objects left, relative to the block prefix.
    pub surviving: Vec<String>,
}

impl StaleListingOutcome {
    /// The outcome that proves a stale listing costs no live block.
    pub fn nothing_lost() -> Self {
        Self {
            while_stale: OrphanSweepStats {
                listed: 2,
                live: 1,
                deleted: 1,
                ..OrphanSweepStats::default()
            },
            once_consistent: OrphanSweepStats {
                listed: 3,
                live: 3,
                ..OrphanSweepStats::default()
            },
            surviving: vec![
                "live.parquet".to_string(),
                "published-late.parquet".to_string(),
                "unpublished.parquet".to_string(),
            ],
        }
    }
}

/// Runs the orphan sweep over `base` while its listing lags two puts behind.
///
/// The sweep runs with no grace window, the harshest setting, so only the
/// listing stands between a block and its deletion.
pub async fn stale_listing(base: Arc<dyn ObjectStore>) -> StaleListingOutcome {
    const PREFIX: &str = "faults/stale/blocks";
    let lag = 2;
    let store = StaleListingStore::new(base, lag);
    let key = |name: &str| format!("{PREFIX}/{name}");
    for name in ["live.parquet", "orphan.parquet"] {
        store
            .put(&Path::from(key(name)), name.into())
            .await
            .expect("the settled objects write");
    }
    for _ in 0..lag {
        let _: Vec<ObjectMeta> = store
            .list(Some(&Path::from(PREFIX)))
            .try_collect()
            .await
            .expect("the warm-up listing reads");
    }
    // Written and not yet in the listing. One is already in the index, as a
    // block published faster than the listing converges. The other is a block
    // whose writer has not published it yet.
    for name in ["published-late.parquet", "unpublished.parquet"] {
        store
            .put(&Path::from(key(name)), name.into())
            .await
            .expect("the fresh objects write");
    }

    let later = SystemTime::now() + Duration::from_mins(1);
    let mut live = BTreeSet::from([key("live.parquet"), key("published-late.parquet")]);
    let while_stale = reconcile_orphans(&store, PREFIX, &live, Time::ZERO, later)
        .await
        .expect("the stale sweep runs");

    for _ in 1..lag {
        let _: Vec<ObjectMeta> = store
            .list(Some(&Path::from(PREFIX)))
            .try_collect()
            .await
            .expect("the catch-up listing reads");
    }
    live.insert(key("unpublished.parquet"));
    let once_consistent = reconcile_orphans(&store, PREFIX, &live, Time::ZERO, later)
        .await
        .expect("the consistent sweep runs");

    let mut surviving: Vec<String> = store
        .list(Some(&Path::from(PREFIX)))
        .map_ok(|meta| {
            meta.location
                .as_ref()
                .trim_start_matches(&format!("{PREFIX}/"))
                .to_string()
        })
        .try_collect()
        .await
        .expect("the final listing reads");
    surviving.sort();
    StaleListingOutcome {
        while_stale,
        once_consistent,
        surviving,
    }
}
