use super::{
    Arc, BoxStream, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreMetrics, ObjectStoreOperation, ObjectStoreRetryPolicy, Path,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, async_trait, retry_object_store,
};

/// An [`ObjectStore`] that retries its single-shot operations when the backend
/// fails transiently.
///
/// Wrap the store a write loop hands to its *index* -- the snapshot manifests,
/// the shard payloads, the compaction sidecar -- so that one 503 in the middle
/// of a flush does not end the role.
///
/// # Do not wrap the store a [`BlockWriter`] holds
///
/// [`BlockWriter`](crate::BlockWriter) retries the whole block write itself,
/// which is the only correct unit for a block: a Parquet block is streamed
/// through a [`BufWriter`](object_store::buffered::BufWriter), and once that
/// has grown past its buffer the write is a multipart upload whose parts
/// cannot be replayed in place -- `object_store` hands out a part number per
/// `put_part` call, so a second call after a failed one leaves a hole that
/// `complete` rejects. Re-driving the whole write from the record batches is
/// what does work, and that is what the writer does. Wrapping its store as
/// well would multiply the two budgets together for the single-`put` case and
/// buy nothing for the multipart one, so this type deliberately passes
/// `put_multipart_opts` straight through.
///
/// `delete_stream` is passed through too. A delete that fails leaves an
/// object behind for the next sweep; it does not lose anything.
#[derive(Debug)]
pub struct RetryingObjectStore {
    inner: Arc<dyn ObjectStore>,
    policy: ObjectStoreRetryPolicy,
    metrics: ObjectStoreMetrics,
}

impl RetryingObjectStore {
    /// Wraps `inner` so its single-shot operations retry under `policy`, and
    /// so every retry moves `metrics`.
    ///
    /// Wrap a store that [`MeteredObjectStore`](crate::MeteredObjectStore)
    /// already wraps, and give both the same `metrics`. The retry count and
    /// the attempt count are then two views of one store.
    ///
    /// The result is already an `Arc<dyn ObjectStore>`, which is the only
    /// shape a caller ever wants it in.
    #[must_use]
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        policy: ObjectStoreRetryPolicy,
        metrics: ObjectStoreMetrics,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            inner,
            policy,
            metrics,
        })
    }
}

impl std::fmt::Display for RetryingObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[async_trait]
impl ObjectStore for RetryingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        retry_object_store(
            self.policy,
            ObjectStoreOperation::Put,
            &self.metrics,
            || {
                self.inner
                    .put_opts(location, payload.clone(), options.clone())
            },
        )
        .await
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
        retry_object_store(
            self.policy,
            ObjectStoreOperation::Get,
            &self.metrics,
            || self.inner.get_opts(location, options.clone()),
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        retry_object_store(
            self.policy,
            ObjectStoreOperation::ListWithDelimiter,
            &self.metrics,
            || self.inner.list_with_delimiter(prefix),
        )
        .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        retry_object_store(
            self.policy,
            ObjectStoreOperation::Copy,
            &self.metrics,
            || self.inner.copy_opts(from, to, options.clone()),
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
}
