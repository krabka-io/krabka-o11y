use std::{
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_query_frontend::{CacheKey, CachePolicy, Clock, ObjectStoreCache, QueryCache};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, memory::InMemory, path::Path,
};

#[derive(Debug, Default)]
struct CountingStore {
    inner: InMemory,
    gets: AtomicUsize,
    downloaded: Arc<AtomicUsize>,
    lists: AtomicUsize,
    delete_gate: Arc<DeleteGate>,
}

#[derive(Debug, Default)]
struct DeleteGate {
    pause: AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CountingStore")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let result = self.inner.get_opts(path, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let downloaded = self.downloaded.clone();
        let stream = result.into_stream().map_ok(move |bytes| {
            downloaded.fetch_add(bytes.len(), Ordering::Relaxed);
            bytes
        });
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let gate = self.delete_gate.clone();
        self.inner.delete_stream(Box::pin(paths.then(move |path| {
            let gate = gate.clone();
            async move {
                if gate.pause.swap(false, Ordering::Relaxed) {
                    gate.started.notify_one();
                    gate.resume.notified().await;
                }
                path
            }
        })))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn now_epoch_millis(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[tokio::test]
async fn insert_downloads_no_payloads_as_objects_bytes_and_tenants_grow() {
    for (objects, payload_bytes, tenants) in [(4, 16, 1), (64, 16, 1), (4, 65_536, 1), (64, 16, 64)]
    {
        let store = Arc::new(CountingStore::default());
        let cache =
            ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_secs(60));
        let payload = "x".repeat(payload_bytes);
        for id in 0_usize..objects {
            cache
                .insert(
                    &CacheKey::new(format!("tenant-{}", id % tenants), id.to_le_bytes()),
                    &payload,
                )
                .await
                .unwrap();
        }
        // A new instance has no process-local metadata to rely on.
        let second =
            ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_secs(60));
        let lists_before = store.lists.load(Ordering::Relaxed);
        let key = CacheKey::new("new-tenant", b"new");
        second.insert(&key, &payload).await.unwrap();
        assert!(store.gets.load(Ordering::Relaxed) == 0);
        assert!(store.downloaded.load(Ordering::Relaxed) == 0);
        assert!(store.lists.load(Ordering::Relaxed) - lists_before == 1);
        assert!(second.get(&key).await.unwrap() == Some(payload));
        assert!(store.gets.load(Ordering::Relaxed) == 1);
        assert!(store.downloaded.load(Ordering::Relaxed) > payload_bytes);
    }
}

#[tokio::test]
async fn replacements_expiration_and_invalidation_keep_only_current_payloads() {
    let store = Arc::new(CountingStore::default());
    let clock = Arc::new(ManualClock::default());
    let first = ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_millis(10))
        .with_clock(clock.clone());
    let second = ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_millis(10))
        .with_clock(clock.clone());
    let key = CacheKey::new("tenant", b"key");
    first.insert(&key, &"old".into()).await.unwrap();
    clock.0.store(8, Ordering::Relaxed);
    second.insert(&key, &"new".into()).await.unwrap();
    // A same-timestamp replacement stays one cache object as well.
    first.insert(&key, &"newer".into()).await.unwrap();
    let objects = store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(objects.len() == 1);
    assert!(first.metrics().snapshot().bytes == usize::try_from(objects[0].size).unwrap());
    clock.0.store(11, Ordering::Relaxed);
    assert!(second.sweep().await.unwrap() == 0);
    assert!(first.get(&key).await.unwrap() == Some("newer".into()));
    clock.0.store(19, Ordering::Relaxed);
    assert!(second.sweep().await.unwrap() == 1);
    assert!(second.metrics().snapshot().bytes == 0);
    first.insert(&key, &"again".into()).await.unwrap();
    assert!(second.invalidate_tenant("tenant").await.unwrap() == 1);
    assert!(first.get(&key).await.unwrap() == None);
    assert!(
        store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    // The only download above was the explicit cache hit.
    assert!(store.gets.load(Ordering::Relaxed) == 1);
}

#[tokio::test]
async fn shared_instances_enforce_byte_limits_and_reject_oversized_replacements() {
    for tenant_limit in [false, true] {
        let store = Arc::new(CountingStore::default());
        let clock = Arc::new(ManualClock::default());
        let policy = CachePolicy {
            max_objects: None,
            max_objects_per_tenant: None,
            max_bytes: if tenant_limit {
                None
            } else {
                NonZeroUsize::new(60)
            },
            max_bytes_per_tenant: if tenant_limit {
                NonZeroUsize::new(60)
            } else {
                None
            },
        };
        let first =
            ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_secs(60))
                .with_clock(clock.clone())
                .with_policy(policy);
        let second =
            ObjectStoreCache::<String>::new(store.clone(), "cache", Duration::from_secs(60))
                .with_clock(clock.clone())
                .with_policy(policy);
        let old = CacheKey::new("tenant", b"old");
        let new = CacheKey::new("tenant", b"new");
        first.insert(&old, &"abcdefghij".into()).await.unwrap();
        clock.0.store(1, Ordering::Relaxed);
        second.insert(&new, &"abcdefghij".into()).await.unwrap();
        assert!(first.get(&old).await.unwrap() == None);
        assert!(second.get(&new).await.unwrap() == Some("abcdefghij".into()));
        first.insert(&new, &"x".repeat(61)).await.unwrap();
        assert!(second.get(&new).await.unwrap() == Some("abcdefghij".into()));
        let total: u64 = store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|object| object.size)
            .sum();
        assert!(total <= 60);
        assert!(second.metrics().snapshot().bytes == usize::try_from(total).unwrap());
        assert!(second.metrics().snapshot().evictions == 1);
    }
}

#[tokio::test]
async fn empty_and_binary_keys_and_tenants_have_distinct_prefixes() {
    let store = Arc::new(CountingStore::default());
    let cache = ObjectStoreCache::<usize>::new(store, "cache", Duration::from_secs(60));
    let keys = [
        CacheKey::new("", b""),
        CacheKey::new("", b"\0"),
        CacheKey::new("a/b", b""),
        CacheKey::new("a", b"b/"),
    ];
    for (id, key) in keys.iter().enumerate() {
        cache.insert(key, &id).await.unwrap();
    }
    for (id, key) in keys.iter().enumerate() {
        assert!(cache.get(key).await.unwrap() == Some(id));
    }
    assert!(cache.invalidate_tenant("").await.unwrap() == 2);
    assert!(cache.get(&keys[2]).await.unwrap() == Some(2));
}

#[tokio::test]
async fn an_expired_snapshot_cannot_delete_a_newer_replacement() {
    let store = Arc::new(CountingStore::default());
    let clock = Arc::new(ManualClock::default());
    let first = Arc::new(
        ObjectStoreCache::<usize>::new(store.clone(), "cache", Duration::from_millis(10))
            .with_clock(clock.clone()),
    );
    let second = ObjectStoreCache::<usize>::new(store.clone(), "cache", Duration::from_millis(10))
        .with_clock(clock.clone());
    let key = CacheKey::new("tenant", b"key");
    first.insert(&key, &1).await.unwrap();
    clock.0.store(11, Ordering::Relaxed);
    store.delete_gate.pause.store(true, Ordering::Relaxed);
    let sweep = tokio::spawn(async move { first.sweep().await });
    store.delete_gate.started.notified().await;
    second.insert(&key, &2).await.unwrap();
    store.delete_gate.resume.notify_one();
    sweep.await.unwrap().unwrap();
    assert!(second.get(&key).await.unwrap() == Some(2));
}

#[tokio::test]
async fn insert_prunes_expired_entries_after_the_exact_ttl_boundary() {
    let store = Arc::new(CountingStore::default());
    let clock = Arc::new(ManualClock::default());
    clock.0.store(-10, Ordering::Relaxed);
    let cache = ObjectStoreCache::<usize>::new(store.clone(), "cache", Duration::from_millis(10))
        .with_clock(clock.clone());
    let old = CacheKey::new("tenant", b"old");
    cache.insert(&old, &1).await.unwrap();
    clock.0.store(0, Ordering::Relaxed);
    cache
        .insert(&CacheKey::new("other", b"boundary"), &2)
        .await
        .unwrap();
    assert!(cache.metrics().snapshot().sweeps == 0);
    clock.0.store(1, Ordering::Relaxed);
    cache
        .insert(&CacheKey::new("other", b"after"), &3)
        .await
        .unwrap();
    assert!(cache.metrics().snapshot().sweeps == 1);
    assert!(cache.get(&old).await.unwrap() == None);
    assert!(store.gets.load(Ordering::Relaxed) == 0);
}

#[tokio::test]
async fn malformed_or_unknown_metadata_versions_are_rejected() {
    for name in ["0.json", "v2-0.json", "v1-invalid.json"] {
        let store = Arc::new(CountingStore::default());
        let cache = ObjectStoreCache::<usize>::new(store.clone(), "cache", Duration::from_secs(60));
        let path = Path::from(format!("cache/74656e616e74/k6b6579/{name}"));
        store
            .inner
            .put(&path, PutPayload::from("{}"))
            .await
            .unwrap();
        assert!(cache.get(&CacheKey::new("tenant", b"key")).await.is_err());
        assert!(cache.sweep().await.is_err());
        assert!(store.gets.load(Ordering::Relaxed) == 0);
        assert!(
            store
                .inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len()
                == 1
        );
    }
}
