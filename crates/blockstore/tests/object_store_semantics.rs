use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::{
    array::{Int64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assert2::{assert, check};
use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP, ConditionalUpdateRequirement,
    OBJECT_STORE_PROBE_PREFIX, ObjectStoreAccess, ObjectStoreCapabilities,
    ObjectStoreSemanticsError, read_block, verify_object_store_access,
    verify_object_store_semantics,
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
    /// Refuses every put and delete, as a read-only credential does.
    ReadOnlyCredential,
    /// Returns one byte fewer than a range asks for.
    ShortRange,
    /// Fails every listing, as a credential without list permission does.
    ListingRefused,
    /// Commits a create, then fails it, as a connection that drops after the
    /// store accepts the request does.
    CreateCommittedThenLost,
    /// Fails a create with `AlreadyExists` because another writer created the
    /// key first.
    CreateRaced,
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

fn permission_denied(location: &Path) -> ObjectStoreError {
    ObjectStoreError::PermissionDenied {
        path: location.to_string(),
        source: "the credential is read-only".into(),
    }
}

fn connection_lost() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "QuirkyStore",
        source: "the connection closed before the response".into(),
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
        if self.quirk == Quirk::ReadOnlyCredential {
            return Err(permission_denied(location));
        }
        match (self.quirk, &options.mode) {
            (Quirk::CreateUnsupported, PutMode::Create) => {
                return Err(not_implemented("put_opts with PutMode::Create"));
            }
            (Quirk::CreateCommittedThenLost, PutMode::Create) => {
                self.inner.put_opts(location, payload, options).await?;
                return Err(connection_lost());
            }
            (Quirk::CreateRaced, PutMode::Create) => {
                self.inner
                    .put_opts(location, "another writer".into(), options)
                    .await?;
                return Err(ObjectStoreError::AlreadyExists {
                    path: location.to_string(),
                    source: "another writer created it".into(),
                });
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
        if self.quirk == Quirk::ShortRange
            && let Some(GetRange::Bounded(range)) = &options.range
        {
            let options = GetOptions {
                range: Some(GetRange::Bounded(range.start..range.end - 1)),
                ..options
            };
            return self.inner.get_opts(location, options).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.quirk == Quirk::ListingLags {
            return futures::stream::empty().boxed();
        }
        if self.quirk == Quirk::ListingRefused {
            let path = prefix.map(ToString::to_string).unwrap_or_default();
            return futures::stream::once(async move { Err(permission_denied(&Path::from(path))) })
                .boxed();
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
        if self.quirk == Quirk::ReadOnlyCredential {
            return locations
                .map(|location| location.and_then(|path| Err(permission_denied(&path))))
                .boxed();
        }
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
async fn a_failed_create_keeps_its_error_and_deletes_only_an_object_the_probe_wrote() {
    // Each case is the quirk, the error the probe returns, and how many
    // objects stay in the store.
    let cases = [
        (
            Quirk::CreateCommittedThenLost,
            "Generic QuirkyStore error: the connection closed before the response",
            0,
        ),
        (
            Quirk::CreateRaced,
            "already exists: another writer created it",
            1,
        ),
    ];
    for (quirk, want_error, want_left) in cases {
        let store = QuirkyStore::new(quirk);

        let result = verify_object_store_semantics(
            &store,
            &Path::from("tenant-data"),
            ConditionalUpdateRequirement::Required,
        )
        .await;

        assert!(let Err(ObjectStoreSemanticsError::ObjectStore { step, source }) = result);
        let error = source.to_string();
        check!(
            (step, error.ends_with(want_error), store.keys().await.len())
                == ("create a probe object", true, want_left),
            "{quirk:?}: {error}"
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

/// Azure has no suffix range, and every block read loads its footer through
/// one. The reader falls back to a bounded range, so a block still reads.
#[tokio::test]
async fn a_block_reads_from_a_store_without_suffix_ranges() {
    let store: Arc<dyn ObjectStore> = Arc::new(QuirkyStore::new(Quirk::SuffixUnsupported));
    let schema = Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![7, 7])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["first", "second"])),
        ],
    )
    .unwrap();
    BlockWriter::new(Arc::clone(&store))
        .write_block(
            "tenant",
            "azure/block.parquet",
            schema,
            std::slice::from_ref(&batch),
        )
        .await
        .unwrap();

    let read = read_block(store, "azure/block.parquet").await.unwrap();

    assert!(read == vec![batch]);
}

/// A read-only role probes with list and get only. It passes on a read-only
/// credential, which the write probe refuses, and it writes nothing.
#[tokio::test]
async fn a_read_only_role_probes_without_writing() {
    const BLOCK: &str = "tenant-data/blocks/a.parquet";
    let cases = [
        ("an empty prefix", Quirk::ReadOnlyCredential, false, Ok(())),
        (
            "a prefix with a block",
            Quirk::ReadOnlyCredential,
            true,
            Ok(()),
        ),
        ("a store with every semantic", Quirk::None, true, Ok(())),
        (
            "a range read that comes back short",
            Quirk::ShortRange,
            true,
            Err("RangedReadMismatch"),
        ),
        (
            "a listing that fails",
            Quirk::ListingRefused,
            false,
            Err("list the configured prefix"),
        ),
    ];
    for (name, quirk, with_block, want) in cases {
        let store = QuirkyStore::new(quirk);
        if with_block {
            object_store::ObjectStoreExt::put(&store.inner, &Path::from(BLOCK), "block".into())
                .await
                .unwrap();
        }
        let before = store.keys().await;

        let result = verify_object_store_access(
            &store,
            &Path::from("tenant-data"),
            ObjectStoreAccess::ReadOnly,
            ConditionalUpdateRequirement::Required,
        )
        .await;

        check!(describe(&result.map(|()| FULL)).map(drop) == want, "{name}");
        check!(
            store.keys().await == before,
            "{name}: the probe writes nothing"
        );
    }

    let refused = verify_object_store_access(
        &QuirkyStore::new(Quirk::ReadOnlyCredential),
        &Path::from("tenant-data"),
        ObjectStoreAccess::ReadWrite,
        ConditionalUpdateRequirement::Required,
    )
    .await;
    check!(
        describe(&refused.map(|()| FULL)) == Err("create a probe object"),
        "the write probe refuses a read-only credential"
    );
}
