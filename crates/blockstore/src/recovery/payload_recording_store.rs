use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use object_store::{
    MultipartUpload, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart, path::Path,
};

/// A store that records the largest payload of one write: a whole put or one
/// multipart part.
#[derive(Debug)]
pub struct PayloadRecordingStore {
    inner: Arc<dyn ObjectStore>,
    largest: Arc<AtomicUsize>,
}

impl PayloadRecordingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            largest: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn largest_payload(&self) -> usize {
        self.largest.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for PayloadRecordingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

crate::delegate_object_store! {
    PayloadRecordingStore => inner;
    forward [get_opts, list, list_with_delimiter, copy_opts, delete_stream];

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.largest
            .fetch_max(payload.content_length(), Ordering::SeqCst);
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(RecordingUpload {
            inner: self.inner.put_multipart_opts(location, options).await?,
            largest: Arc::clone(&self.largest),
        }))
    }
}

#[derive(Debug)]
struct RecordingUpload {
    inner: Box<dyn MultipartUpload>,
    largest: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for RecordingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.largest
            .fetch_max(data.content_length(), Ordering::SeqCst);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}
