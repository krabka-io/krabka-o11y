// An in-memory object store that runs a test's hooks before it serves a
// request, so a test store states only what it does differently.

use std::{fmt, ops::Deref};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

// What a test store does before the in-memory store serves a request.
#[async_trait::async_trait]
pub trait StoreHooks: Send + Sync + 'static {
    // The name the store reports through `Debug` and `Display`.
    const NAME: &'static str;

    // Runs before each put. An error fails the put, and nothing is stored.
    async fn before_put(&self, location: &Path, payload: &PutPayload) -> object_store::Result<()>;

    // Runs before each get.
    fn before_get(&self, _location: &Path) {}
}

// An [`InMemory`] store behind `H`'s hooks. It dereferences to the hooks, so
// a test reads what they recorded through the store itself.
pub struct HookedStore<H> {
    inner: InMemory,
    hooks: H,
}

impl<H> HookedStore<H> {
    pub fn new(hooks: H) -> Self {
        Self {
            inner: InMemory::new(),
            hooks,
        }
    }
}

impl<H> Deref for HookedStore<H> {
    type Target = H;

    fn deref(&self) -> &H {
        &self.hooks
    }
}

impl<H: StoreHooks> fmt::Debug for HookedStore<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(H::NAME)
    }
}

impl<H: StoreHooks> fmt::Display for HookedStore<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(H::NAME)
    }
}

#[async_trait::async_trait]
impl<H: StoreHooks> ObjectStore for HookedStore<H> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.hooks.before_put(location, &payload).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.hooks.before_get(location);
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
