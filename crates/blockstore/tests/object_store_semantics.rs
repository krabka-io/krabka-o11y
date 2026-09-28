use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use assert2::{assert, check};
use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    ConditionalUpdateRequirement, OBJECT_STORE_PROBE_PREFIX, ObjectStoreCapabilities,
    ObjectStoreSemanticsError, verify_object_store_semantics,
};
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetRange, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, UpdateVersion, local::LocalFileSystem, memory::InMemory, path::Path,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Quirk {
    None,
    CreateUnsupported,
    CreateIgnored,
    UpdateIgnored,
    ListingLags,
    SuffixUnsupported,
    /// Matches an update on `version` alone and ignores `e_tag`, as GCS
    /// matches on the object generation.
    VersionMatchedUpdate,
}

#[derive(Debug)]
struct QuirkyStore {
    inner: InMemory,
    quirk: Quirk,
    generation: AtomicU64,
}

impl QuirkyStore {
    fn new(quirk: Quirk) -> Self {
        Self {
            inner: InMemory::new(),
            quirk,
            generation: AtomicU64::new(0),
        }
    }

    async fn keys(&self) -> Vec<String> {
        self.inner
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap()
    }
}

impl std::fmt::Display for QuirkyStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "QuirkyStore({:?})", self.quirk)
    }
}

fn not_implemented(operation: &str) -> ObjectStoreError {
    ObjectStoreError::NotImplemented {
        operation: operation.to_string(),
        implementer: "QuirkyStore".to_string(),
    }
}

#[async_trait]
impl ObjectStore for QuirkyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> object_store::Result<PutResult> {
        match (self.quirk, &options.mode) {
            (Quirk::CreateUnsupported, PutMode::Create) => {
                return Err(not_implemented("put_opts with PutMode::Create"));
            }
            (Quirk::CreateIgnored, PutMode::Create)
            | (Quirk::UpdateIgnored, PutMode::Update(_)) => options.mode = PutMode::Overwrite,
            (Quirk::VersionMatchedUpdate, PutMode::Update(version)) => {
                let Some(generation) = version.version.clone() else {
                    return Err(ObjectStoreError::Generic {
                        store: "QuirkyStore",
                        source: "a conditional update needs the object generation".into(),
                    });
                };
                options.mode = PutMode::Update(UpdateVersion {
                    e_tag: Some(generation),
                    version: None,
                });
            }
            _ => {}
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        if self.quirk == Quirk::VersionMatchedUpdate {
            // The generation is the only thing that matches, so the ETag this
            // store hands out is one no later update can use.
            let bogus = self.generation.fetch_add(1, Ordering::SeqCst);
            return Ok(PutResult {
                e_tag: Some(format!("unmatchable-{bogus}")),
                version: result.e_tag,
            });
        }
        Ok(result)
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
        if self.quirk == Quirk::SuffixUnsupported
            && matches!(options.range, Some(GetRange::Suffix(_)))
        {
            return Err(ObjectStoreError::NotSupported {
                source: "suffix range requests".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.quirk == Quirk::ListingLags {
            return futures::stream::empty().boxed();
        }
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

fn describe(
    result: &Result<ObjectStoreCapabilities, ObjectStoreSemanticsError>,
) -> Result<ObjectStoreCapabilities, &'static str> {
    match result {
        Ok(capabilities) => Ok(*capabilities),
        Err(ObjectStoreSemanticsError::CreateIfAbsentUnsupported { .. }) => {
            Err("CreateIfAbsentUnsupported")
        }
        Err(ObjectStoreSemanticsError::CreateIfAbsentIgnored { .. }) => {
            Err("CreateIfAbsentIgnored")
        }
        Err(ObjectStoreSemanticsError::ConditionalUpdateUnsupported { .. }) => {
            Err("ConditionalUpdateUnsupported")
        }
        Err(ObjectStoreSemanticsError::ConditionalUpdateIgnored { .. }) => {
            Err("ConditionalUpdateIgnored")
        }
        Err(ObjectStoreSemanticsError::RangedReadMismatch { .. }) => Err("RangedReadMismatch"),
        Err(ObjectStoreSemanticsError::ListingMissedWrite { .. }) => Err("ListingMissedWrite"),
        Err(ObjectStoreSemanticsError::DeleteNotIdempotent { .. }) => Err("DeleteNotIdempotent"),
        Err(ObjectStoreSemanticsError::ObjectStore { step, .. }) => Err(step),
        Err(_) => Err("an error this suite does not know"),
    }
}

const FULL: ObjectStoreCapabilities = ObjectStoreCapabilities {
    conditional_update: true,
    suffix_range_read: true,
};

#[tokio::test]
async fn the_probe_names_the_first_semantic_a_store_is_missing() {
    let cases = [
        (
            Quirk::None,
            ConditionalUpdateRequirement::Required,
            Ok(FULL),
        ),
        (
            Quirk::VersionMatchedUpdate,
            ConditionalUpdateRequirement::Required,
            Ok(FULL),
        ),
        (
            Quirk::SuffixUnsupported,
            ConditionalUpdateRequirement::Required,
            Ok(ObjectStoreCapabilities {
                conditional_update: true,
                suffix_range_read: false,
            }),
        ),
        (
            Quirk::CreateUnsupported,
            ConditionalUpdateRequirement::Optional,
            Err("CreateIfAbsentUnsupported"),
        ),
        (
            Quirk::CreateIgnored,
            ConditionalUpdateRequirement::Optional,
            Err("CreateIfAbsentIgnored"),
        ),
        (
            Quirk::UpdateIgnored,
            ConditionalUpdateRequirement::Optional,
            Err("ConditionalUpdateIgnored"),
        ),
        (
            Quirk::ListingLags,
            ConditionalUpdateRequirement::Required,
            Err("ListingMissedWrite"),
        ),
    ];
    for (quirk, requirement, want) in cases {
        let store = QuirkyStore::new(quirk);
        let result =
            verify_object_store_semantics(&store, &Path::from("tenant-data"), requirement).await;
        check!(describe(&result) == want, "{quirk:?}");
        check!(
            store.keys().await.is_empty(),
            "{quirk:?}: the probe leaves no object behind"
        );
    }
}

#[tokio::test]
async fn a_local_filesystem_passes_only_when_a_conditional_update_is_optional() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let cases = [
        (
            ConditionalUpdateRequirement::Optional,
            Ok(ObjectStoreCapabilities {
                conditional_update: false,
                suffix_range_read: true,
            }),
        ),
        (
            ConditionalUpdateRequirement::Required,
            Err("ConditionalUpdateUnsupported"),
        ),
    ];
    for (requirement, want) in cases {
        let result = verify_object_store_semantics(&store, &Path::from(""), requirement).await;
        check!(describe(&result) == want, "{requirement:?}");
        let left: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        check!(
            left.is_empty(),
            "{requirement:?}: the probe leaves no object behind"
        );
    }
}

#[tokio::test]
async fn the_probe_writes_below_its_own_prefix_and_leaves_the_rest_alone() {
    let store = Arc::new(InMemory::new());
    let data = Path::from("tenant-data/blocks/a.parquet");
    object_store::ObjectStoreExt::put(store.as_ref(), &data, "block".into())
        .await
        .unwrap();

    let capabilities = verify_object_store_semantics(
        store.as_ref(),
        &Path::from("tenant-data"),
        ConditionalUpdateRequirement::Required,
    )
    .await
    .unwrap();

    assert!(capabilities == FULL);
    let left: Vec<String> = store
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .unwrap();
    assert!(left == vec![data.to_string()]);
    assert!(OBJECT_STORE_PROBE_PREFIX == ".krabka-probe");
}

#[test]
fn only_a_store_one_process_owns_may_skip_the_conditional_update() {
    let cases = [
        (
            "file:///var/lib/krabka",
            ConditionalUpdateRequirement::Optional,
        ),
        ("memory:///", ConditionalUpdateRequirement::Optional),
        ("./data", ConditionalUpdateRequirement::Optional),
        ("/var/lib/krabka", ConditionalUpdateRequirement::Optional),
        ("s3://bucket/prefix", ConditionalUpdateRequirement::Required),
        ("gs://bucket/prefix", ConditionalUpdateRequirement::Required),
        (
            "az://container/prefix",
            ConditionalUpdateRequirement::Required,
        ),
        (
            "https://account.blob.core.windows.net/container",
            ConditionalUpdateRequirement::Required,
        ),
    ];
    for (url, want) in cases {
        check!(
            ConditionalUpdateRequirement::for_object_store_url(url) == want,
            "{url}"
        );
    }
}
