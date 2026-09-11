use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::check;
use futures::StreamExt as _;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory,
    path::Path,
};
use prometheus_client::registry::Registry;

use super::{MeteredObjectStore, ObjectStoreMetrics, ObjectStoreOperation};

/// An in-memory store whose `put_opts` and `list` fail on demand.
#[derive(Debug)]
struct BreakableStore {
    inner: Arc<InMemory>,
    failing_puts: AtomicUsize,
    fail_list: bool,
}

impl BreakableStore {
    fn new(failing_puts: usize, fail_list: bool) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            failing_puts: AtomicUsize::new(failing_puts),
            fail_list,
        }
    }
}

fn transient() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "test",
        source: "503 from the backend".into(),
    }
}

impl std::fmt::Display for BreakableStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BreakableStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for BreakableStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self
            .failing_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                (left > 0).then(|| left - 1)
            })
            .is_ok()
        {
            return Err(transient());
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        if self.fail_list {
            return futures::stream::once(async { Err(transient()) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
}

#[tokio::test]
async fn a_put_and_a_get_move_their_own_operation_series_only() {
    let metrics = ObjectStoreMetrics::unregistered();
    let store = MeteredObjectStore::wrap(Arc::new(BreakableStore::new(0, false)), metrics.clone());

    store
        .put(
            &Path::from("blocks/a"),
            PutPayload::from_static(b"0123456789"),
        )
        .await
        .expect("the in-memory store accepts a put");

    check!(metrics.operations(ObjectStoreOperation::Put) == 1);
    check!(metrics.failures(ObjectStoreOperation::Put) == 0);
    check!(metrics.transferred_bytes(ObjectStoreOperation::Put) == 10);
    check!(
        metrics.operations(ObjectStoreOperation::Get) == 0,
        "a put must not move the get series"
    );

    let read = store
        .get(&Path::from("blocks/a"))
        .await
        .expect("the object is there");
    let bytes = read.bytes().await.expect("reading it back");

    check!(bytes.len() == 10);
    check!(metrics.operations(ObjectStoreOperation::Get) == 1);
    check!(metrics.transferred_bytes(ObjectStoreOperation::Get) == 10);
    check!(
        metrics.operations(ObjectStoreOperation::Put) == 1,
        "a get must not move the put series"
    );
}

#[tokio::test]
async fn a_failed_put_counts_as_an_attempt_and_as_a_failure() {
    let metrics = ObjectStoreMetrics::unregistered();
    let store = MeteredObjectStore::wrap(Arc::new(BreakableStore::new(1, false)), metrics.clone());

    let outcome = store
        .put(&Path::from("blocks/a"), PutPayload::from_static(b"x"))
        .await;

    check!(outcome.is_err());
    check!(metrics.operations(ObjectStoreOperation::Put) == 1);
    check!(metrics.failures(ObjectStoreOperation::Put) == 1);
    check!(
        metrics.transferred_bytes(ObjectStoreOperation::Put) == 0,
        "a put that failed transferred nothing"
    );
}

#[tokio::test]
async fn a_listing_is_recorded_when_the_stream_ends_and_not_before() {
    let metrics = ObjectStoreMetrics::unregistered();
    let store = MeteredObjectStore::wrap(Arc::new(BreakableStore::new(0, false)), metrics.clone());
    store
        .put(&Path::from("blocks/a"), PutPayload::from_static(b"x"))
        .await
        .expect("a put");

    let mut listing = store.list(None);
    let first = listing.next().await;

    check!(first.is_some());
    check!(
        metrics.operations(ObjectStoreOperation::List) == 0,
        "a listing that has not finished has no duration to report"
    );

    while listing.next().await.is_some() {}
    drop(listing);

    check!(metrics.operations(ObjectStoreOperation::List) == 1);
    check!(metrics.failures(ObjectStoreOperation::List) == 0);
}

#[tokio::test]
async fn a_listing_that_errors_is_recorded_as_a_failure() {
    let metrics = ObjectStoreMetrics::unregistered();
    let store = MeteredObjectStore::wrap(Arc::new(BreakableStore::new(0, true)), metrics.clone());

    let items: Vec<_> = store.list(None).collect().await;

    check!(items.len() == 1);
    check!(items[0].is_err());
    check!(metrics.operations(ObjectStoreOperation::List) == 1);
    check!(metrics.failures(ObjectStoreOperation::List) == 1);
}

/// The instruments are read by scraping the registry, not by reading the
/// handles back. A metric that is registered under the wrong name, that is
/// registered into the wrong sub-registry, or that is never registered at all
/// passes every handle assertion above and fails here. That is the failure
/// mode this module exists to stop, because a counter nobody registered scrapes
/// exactly like a counter nobody incremented.
///
/// Every series checked here is non-zero, so the test also fails when a
/// recording call is dropped from the decorator. A `prometheus-client`
/// `Family` emits no series for a label set nothing has touched, so asserting
/// on a zero would assert on a line that is absent either way.
#[tokio::test]
async fn every_object_store_instrument_reaches_a_scrape_under_the_service_prefix() {
    let mut registry = Registry::with_prefix("krabka_test");
    let metrics = ObjectStoreMetrics::register(&mut registry);
    let store = MeteredObjectStore::wrap(Arc::new(BreakableStore::new(1, false)), metrics.clone());

    // The first put fails, the second succeeds, so both outcome series exist.
    let failed = store
        .put(
            &Path::from("blocks/a"),
            PutPayload::from_static(b"0123456789"),
        )
        .await;
    check!(failed.is_err());
    store
        .put(
            &Path::from("blocks/a"),
            PutPayload::from_static(b"0123456789"),
        )
        .await
        .expect("the second put succeeds");
    metrics.record_retry(ObjectStoreOperation::Put);

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encoding");

    for needle in [
        "krabka_test_objstore_operations_total{operation=\"put\"} 2",
        "krabka_test_objstore_operation_failures_total{operation=\"put\"} 1",
        "krabka_test_objstore_operation_retries_total{operation=\"put\"} 1",
        "krabka_test_objstore_operation_transferred_bytes_total{operation=\"put\"} 10",
        "krabka_test_objstore_operation_duration_seconds_count{operation=\"put\"} 2",
    ] {
        check!(buffer.contains(needle), "missing {needle} in:\n{buffer}");
    }
}

#[test]
fn every_operation_has_its_own_label_value() {
    let mut seen: Vec<&str> = ObjectStoreOperation::all()
        .into_iter()
        .map(ObjectStoreOperation::as_str)
        .collect();
    let count = seen.len();
    seen.sort_unstable();
    seen.dedup();
    check!(seen.len() == count, "two operations share a label value");
}
