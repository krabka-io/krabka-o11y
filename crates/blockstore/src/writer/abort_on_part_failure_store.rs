use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{FutureExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, path::Path,
};

#[derive(Debug)]
pub(super) struct AbortOnPartFailureStore {
    inner: Arc<dyn ObjectStore>,
}

impl AbortOnPartFailureStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for AbortOnPartFailureStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[async_trait]
impl ObjectStore for AbortOnPartFailureStore {
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
        let upload = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(AbortOnPartFailureUpload {
            inner: Arc::new(Mutex::new(Some(upload))),
        }))
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
        self.inner.delete_stream(locations)
    }
}

#[derive(Debug)]
struct AbortOnPartFailureUpload {
    inner: Arc<Mutex<Option<Box<dyn MultipartUpload>>>>,
}

#[async_trait]
impl MultipartUpload for AbortOnPartFailureUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = match self.inner.lock() {
            Ok(mut inner) => match inner.as_mut() {
                Some(upload) => upload.put_part(data),
                None => return async { Err(upload_closed()) }.boxed(),
            },
            Err(_) => return async { Err(upload_poisoned()) }.boxed(),
        };
        let inner = Arc::clone(&self.inner);
        async move {
            if let Err(error) = part.await {
                let upload = inner.lock().map_err(|_| upload_poisoned())?.take();
                if let Some(mut upload) = upload {
                    upload.abort().await?;
                }
                return Err(error);
            }
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut upload = self
            .inner
            .lock()
            .map_err(|_| upload_poisoned())?
            .take()
            .ok_or_else(upload_closed)?;
        let result = upload.complete().await;
        if result.is_err() {
            *self.inner.lock().map_err(|_| upload_poisoned())? = Some(upload);
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let upload = self.inner.lock().map_err(|_| upload_poisoned())?.take();
        match upload {
            Some(mut upload) => upload.abort().await,
            None => Ok(()),
        }
    }
}

fn upload_closed() -> object_store::Error {
    object_store::Error::Generic {
        store: "multipart upload",
        source: std::io::Error::other("upload already completed or aborted").into(),
    }
}

fn upload_poisoned() -> object_store::Error {
    object_store::Error::Generic {
        store: "multipart upload",
        source: std::io::Error::other("upload state lock poisoned").into(),
    }
}
