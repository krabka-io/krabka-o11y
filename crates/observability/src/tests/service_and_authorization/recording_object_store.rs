use super::*;

#[derive(Clone)]
pub(crate) struct RecordingObjectStore {
    pub(crate) inner: Arc<object_store::memory::InMemory>,
    pub(crate) put_paths: Arc<Mutex<Vec<String>>>,
    pub(crate) get_paths: Arc<Mutex<Vec<String>>>,
    pub(crate) list_prefixes: Arc<Mutex<Vec<String>>>,
    pub(crate) list_offsets: Arc<Mutex<Vec<String>>>,
    pub(crate) get_delay: Duration,
    pub(crate) active_gets: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) max_active_gets: Arc<std::sync::atomic::AtomicUsize>,
}

impl RecordingObjectStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            put_paths: Arc::new(Mutex::new(Vec::new())),
            get_paths: Arc::new(Mutex::new(Vec::new())),
            list_prefixes: Arc::new(Mutex::new(Vec::new())),
            list_offsets: Arc::new(Mutex::new(Vec::new())),
            get_delay: Duration::ZERO,
            active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub(crate) fn with_get_delay(mut self, get_delay: Duration) -> Self {
        self.get_delay = get_delay;
        self
    }

    pub(crate) fn clear_recorded_paths(&self) {
        self.put_paths.lock().unwrap().clear();
        self.get_paths.lock().unwrap().clear();
        self.list_prefixes.lock().unwrap().clear();
        self.list_offsets.lock().unwrap().clear();
    }

    pub(crate) fn clear_put_paths(&self) {
        self.put_paths.lock().unwrap().clear();
    }

    pub(crate) fn put_paths(&self) -> Vec<String> {
        self.put_paths.lock().unwrap().clone()
    }

    pub(crate) fn get_paths(&self) -> Vec<String> {
        self.get_paths.lock().unwrap().clone()
    }

    pub(crate) fn list_prefixes(&self) -> Vec<String> {
        self.list_prefixes.lock().unwrap().clone()
    }

    pub(crate) fn list_offsets(&self) -> Vec<String> {
        self.list_offsets.lock().unwrap().clone()
    }

    pub(crate) fn max_active_gets(&self) -> usize {
        self.max_active_gets
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn record_get_start(&self) {
        let active = self
            .active_gets
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let mut current = self
            .max_active_gets
            .load(std::sync::atomic::Ordering::SeqCst);
        while active > current {
            match self.max_active_gets.compare_exchange(
                current,
                active,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn record_get_end(&self) {
        self.active_gets
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl std::fmt::Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

impl std::fmt::Display for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.put_paths.lock().unwrap().push(location.to_string());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.get_paths.lock().unwrap().push(location.to_string());
        self.record_get_start();
        if !self.get_delay.is_zero() {
            sleep(self.get_delay).await;
        }
        let result = self.inner.get_opts(location, options).await;
        self.record_get_end();
        result
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.list_prefixes
            .lock()
            .unwrap()
            .push(prefix.map_or_else(String::new, ToString::to_string));
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.list_prefixes
            .lock()
            .unwrap()
            .push(prefix.map_or_else(String::new, ToString::to_string));
        self.list_offsets.lock().unwrap().push(offset.to_string());
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
