use super::{
    Arc, BoxStream, ByteSize, ByteSizeExt, CopyOptions, GetOptions, GetResult, Instant, ListResult,
    MeteredStream, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreMetrics,
    ObjectStoreOperation, Path, PutMultipartOptions, PutOptions, PutPayload, PutResult, Time,
    TimeExt, async_trait,
};

/// An [`ObjectStore`] that counts and times every request it passes through.
///
/// Wrap the store once, where the service builds it, and before anything else
/// wraps it. Every reader and writer in this crate then goes through one
/// decorator, and `DataFusion` does too, because the store it registers by URL
/// is this one.
///
/// # Where this sits relative to [`RetryingObjectStore`]
///
/// This decorator goes on the inside:
/// `RetryingObjectStore::wrap(MeteredObjectStore::wrap(raw, metrics), policy)`.
/// So it sees one request per *attempt*, not one per operation. That is the
/// reading an operator wants from a request rate and a latency histogram: it
/// is what the store was actually asked to do, and a store that is failing and
/// being retried shows the load it is under rather than the load the caller
/// meant to put on it.
///
/// `operation_failures_total` therefore counts failed attempts, including the
/// transient ones a later attempt recovered from. The operation that finally
/// failed is not separable from that counter alone. It is separable with
/// `operation_retries_total`, which
/// [`retry_object_store`](crate::retry_object_store) moves for every retry it
/// fires: failures that are all matched by retries are a store that is
/// degrading and still working, and failures in excess of retries are
/// operations the caller saw fail.
///
/// [`RetryingObjectStore`]: crate::RetryingObjectStore
#[derive(Debug)]
pub struct MeteredObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: ObjectStoreMetrics,
}

impl MeteredObjectStore {
    /// Wraps `inner` so its requests land in `metrics`.
    ///
    /// The result is already an `Arc<dyn ObjectStore>`, which is the only
    /// shape a caller ever wants it in.
    #[must_use]
    pub fn wrap(inner: Arc<dyn ObjectStore>, metrics: ObjectStoreMetrics) -> Arc<dyn ObjectStore> {
        Arc::new(Self { inner, metrics })
    }

    /// Records one attempt that has just finished.
    fn record<T, E>(
        &self,
        operation: ObjectStoreOperation,
        started: Instant,
        outcome: &Result<T, E>,
    ) {
        self.metrics.record_operation(
            operation,
            outcome.is_ok(),
            Time::from_std(started.elapsed()),
        );
    }
}

impl std::fmt::Display for MeteredObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[async_trait]
impl ObjectStore for MeteredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let size = ByteSize::from_bytes(payload.content_length() as u64);
        let started = Instant::now();
        let outcome = self.inner.put_opts(location, payload, options).await;
        self.record(ObjectStoreOperation::Put, started, &outcome);
        if outcome.is_ok() {
            self.metrics
                .record_transferred(ObjectStoreOperation::Put, size);
        }
        outcome
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Only the handshake that opens the upload is timed. The parts that
        // follow go to the `MultipartUpload` the caller now holds, which this
        // decorator does not own, so their bytes are not counted here.
        let started = Instant::now();
        let outcome = self.inner.put_multipart_opts(location, options).await;
        self.record(ObjectStoreOperation::PutMultipart, started, &outcome);
        outcome
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let started = Instant::now();
        let outcome = self.inner.get_opts(location, options).await;
        self.record(ObjectStoreOperation::Get, started, &outcome);
        if let Ok(result) = &outcome {
            // `meta.size` is the whole object. A ranged read transfers only
            // the range, so the range width is the honest number when there
            // is one.
            let size = result
                .range
                .end
                .checked_sub(result.range.start)
                .unwrap_or(result.meta.size);
            self.metrics
                .record_transferred(ObjectStoreOperation::Get, ByteSize::from_bytes(size));
        }
        outcome
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Box::pin(MeteredStream::new(
            self.inner.list(prefix),
            self.metrics.clone(),
            ObjectStoreOperation::List,
        ))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let started = Instant::now();
        let outcome = self.inner.list_with_delimiter(prefix).await;
        self.record(ObjectStoreOperation::ListWithDelimiter, started, &outcome);
        outcome
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let started = Instant::now();
        let outcome = self.inner.copy_opts(from, to, options).await;
        self.record(ObjectStoreOperation::Copy, started, &outcome);
        outcome
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        Box::pin(MeteredStream::new(
            self.inner.delete_stream(locations),
            self.metrics.clone(),
            ObjectStoreOperation::DeleteStream,
        ))
    }
}
