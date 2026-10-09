use std::sync::Mutex;

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutResult,
};
use tokio::sync::Notify;

use super::*;

/// An object store that counts the requests made through it, by kind.
///
/// `MeteredObjectStore` counts a `head` and a `get` as one operation. A read
/// amplification test has to tell them apart, because the fixes it guards
/// remove `head` requests and range reads separately.
#[derive(Debug)]
pub(crate) struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<Mutex<RequestCounts>>,
    read_pause: Mutex<Option<[Arc<Notify>; 2]>>,
}

impl CountingObjectStore {
    pub(crate) fn wrap(
        inner: Arc<dyn ObjectStore>,
    ) -> (Arc<dyn ObjectStore>, Arc<Mutex<RequestCounts>>) {
        let counts = Arc::new(Mutex::new(RequestCounts::default()));
        let store = Arc::new(Self {
            inner,
            counts: Arc::clone(&counts),
            read_pause: Mutex::new(None),
        });
        (store, counts)
    }

    pub(crate) fn wrap_paused(
        inner: Arc<dyn ObjectStore>,
    ) -> (Arc<dyn ObjectStore>, [Arc<Notify>; 2]) {
        let signals = [Arc::new(Notify::new()), Arc::new(Notify::new())];
        let store = Arc::new(Self {
            inner,
            counts: Arc::new(Mutex::new(RequestCounts::default())),
            read_pause: Mutex::new(Some(signals.clone())),
        });
        (store, signals)
    }

    fn count(&self, bump: impl FnOnce(&mut RequestCounts)) {
        bump(&mut self.counts.lock().unwrap());
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    // `get_ranges` keeps its default, which issues one ranged `get_opts` per
    // coalesced range. That is the request count a remote store would see.
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let pause = self.read_pause.lock().unwrap().take();
        if let Some([started, resumed]) = pause {
            started.notify_one();
            resumed.notified().await;
        }
        if options.head {
            self.count(|counts| counts.heads += 1);
        } else if options.range.is_some() {
            self.count(|counts| counts.range_gets += 1);
        } else {
            self.count(|counts| counts.full_gets += 1);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.count(|counts| counts.lists += 1);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.count(|counts| counts.lists += 1);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.count(|counts| counts.lists += 1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
