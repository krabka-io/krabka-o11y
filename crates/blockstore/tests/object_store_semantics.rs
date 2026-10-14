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
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP, ConditionalUpdateRequirement,
    OBJECT_STORE_PROBE_PREFIX, ObjectStoreAccess, ObjectStoreCapabilities,
    ObjectStoreSemanticsError, read_block, verify_object_store_access,
    verify_object_store_semantics,
};
use object_store::{
    Error as ObjectStoreError, GetOptions, GetRange, GetResult, ObjectMeta, ObjectStore, PutMode,
    PutOptions, PutPayload, PutResult, UpdateVersion, local::LocalFileSystem, memory::InMemory,
    path::Path,
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

struct ProbeCase {
    quirk: Quirk,
    requirement: ConditionalUpdateRequirement,
    want: Result<ObjectStoreCapabilities, &'static str>,
}

struct CreateFailureCase {
    quirk: Quirk,
    want_error: &'static str,
    want_left: usize,
}

struct RequirementCase {
    requirement: ConditionalUpdateRequirement,
    want: Result<ObjectStoreCapabilities, &'static str>,
}

struct UrlRequirementCase {
    url: &'static str,
    requirement: ConditionalUpdateRequirement,
}

struct ReadOnlyCase {
    name: &'static str,
    quirk: Quirk,
    with_block: bool,
    want: Result<(), &'static str>,
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

krabka_blockstore::delegate_object_store! {
    QuirkyStore => inner;
    forward [put_multipart_opts, list_with_delimiter, copy_opts];

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
                extensions: result.extensions,
            });
        }
        Ok(result)
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
            let path = prefix.map_or_else(Default::default, ToString::to_string);
            return futures::stream::once(async move { Err(permission_denied(&Path::from(path))) })
                .boxed();
        }
        self.inner.list(prefix)
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
        ProbeCase {
            quirk: Quirk::None,
            requirement: ConditionalUpdateRequirement::Required,
            want: Ok(FULL),
        },
        ProbeCase {
            quirk: Quirk::VersionMatchedUpdate,
            requirement: ConditionalUpdateRequirement::Required,
            want: Ok(FULL),
        },
        ProbeCase {
            quirk: Quirk::SuffixUnsupported,
            requirement: ConditionalUpdateRequirement::Required,
            want: Ok(ObjectStoreCapabilities {
                conditional_update: true,
                suffix_range_read: false,
            }),
        },
        ProbeCase {
            quirk: Quirk::CreateUnsupported,
            requirement: ConditionalUpdateRequirement::Optional,
            want: Err("CreateIfAbsentUnsupported"),
        },
        ProbeCase {
            quirk: Quirk::CreateIgnored,
            requirement: ConditionalUpdateRequirement::Optional,
            want: Err("CreateIfAbsentIgnored"),
        },
        ProbeCase {
            quirk: Quirk::UpdateIgnored,
            requirement: ConditionalUpdateRequirement::Optional,
            want: Err("ConditionalUpdateIgnored"),
        },
        ProbeCase {
            quirk: Quirk::ListingLags,
            requirement: ConditionalUpdateRequirement::Required,
            want: Err("ListingMissedWrite"),
        },
    ];
    for case in cases {
        let store = QuirkyStore::new(case.quirk);
        let result =
            verify_object_store_semantics(&store, &Path::from("tenant-data"), case.requirement)
                .await;
        check!(describe(&result) == case.want, "{:?}", case.quirk);
        check!(
            store.keys().await.is_empty(),
            "{:?}: the probe leaves no object behind",
            case.quirk
        );
    }
}

#[tokio::test]
async fn a_failed_create_keeps_its_error_and_deletes_only_an_object_the_probe_wrote() {
    // Each case is the quirk, the error the probe returns, and how many
    // objects stay in the store.
    let cases = [
        CreateFailureCase {
            quirk: Quirk::CreateCommittedThenLost,
            want_error: "Generic QuirkyStore error: the connection closed before the response",
            want_left: 0,
        },
        CreateFailureCase {
            quirk: Quirk::CreateRaced,
            want_error: "already exists: another writer created it",
            want_left: 1,
        },
    ];
    for case in cases {
        let store = QuirkyStore::new(case.quirk);

        let result = verify_object_store_semantics(
            &store,
            &Path::from("tenant-data"),
            ConditionalUpdateRequirement::Required,
        )
        .await;

        assert!(let Err(ObjectStoreSemanticsError::ObjectStore { step, source }) = result);
        let error = source.to_string();
        check!(
            step == "create a probe object"
                && error.ends_with(case.want_error)
                && store.keys().await.len() == case.want_left,
            "{:?}: {error}",
            case.quirk
        );
    }
}

#[tokio::test]
async fn a_local_filesystem_passes_only_when_a_conditional_update_is_optional() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let cases = [
        RequirementCase {
            requirement: ConditionalUpdateRequirement::Optional,
            want: Ok(ObjectStoreCapabilities {
                conditional_update: false,
                suffix_range_read: true,
            }),
        },
        RequirementCase {
            requirement: ConditionalUpdateRequirement::Required,
            want: Err("ConditionalUpdateUnsupported"),
        },
    ];
    for case in cases {
        let result = verify_object_store_semantics(&store, &Path::from(""), case.requirement).await;
        check!(describe(&result) == case.want, "{:?}", case.requirement);
        let left: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        check!(
            left.is_empty(),
            "{:?}: the probe leaves no object behind",
            case.requirement
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
        UrlRequirementCase {
            url: "file:///var/lib/krabka",
            requirement: ConditionalUpdateRequirement::Optional,
        },
        UrlRequirementCase {
            url: "memory:///",
            requirement: ConditionalUpdateRequirement::Optional,
        },
        UrlRequirementCase {
            url: "./data",
            requirement: ConditionalUpdateRequirement::Optional,
        },
        UrlRequirementCase {
            url: "/var/lib/krabka",
            requirement: ConditionalUpdateRequirement::Optional,
        },
        UrlRequirementCase {
            url: "s3://bucket/prefix",
            requirement: ConditionalUpdateRequirement::Required,
        },
        UrlRequirementCase {
            url: "gs://bucket/prefix",
            requirement: ConditionalUpdateRequirement::Required,
        },
        UrlRequirementCase {
            url: "az://container/prefix",
            requirement: ConditionalUpdateRequirement::Required,
        },
        UrlRequirementCase {
            url: "https://account.blob.core.windows.net/container",
            requirement: ConditionalUpdateRequirement::Required,
        },
    ];
    for case in cases {
        check!(
            ConditionalUpdateRequirement::for_object_store_url(case.url) == case.requirement,
            "{}",
            case.url
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
        ReadOnlyCase {
            name: "an empty prefix",
            quirk: Quirk::ReadOnlyCredential,
            with_block: false,
            want: Ok(()),
        },
        ReadOnlyCase {
            name: "a prefix with a block",
            quirk: Quirk::ReadOnlyCredential,
            with_block: true,
            want: Ok(()),
        },
        ReadOnlyCase {
            name: "a store with every semantic",
            quirk: Quirk::None,
            with_block: true,
            want: Ok(()),
        },
        ReadOnlyCase {
            name: "a range read that comes back short",
            quirk: Quirk::ShortRange,
            with_block: true,
            want: Err("RangedReadMismatch"),
        },
        ReadOnlyCase {
            name: "a listing that fails",
            quirk: Quirk::ListingRefused,
            with_block: false,
            want: Err("list the configured prefix"),
        },
    ];
    for case in cases {
        let store = QuirkyStore::new(case.quirk);
        if case.with_block {
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

        check!(
            describe(&result.map(|()| FULL)).map(drop) == case.want,
            "{}",
            case.name
        );
        check!(
            store.keys().await == before,
            "{}: the probe writes nothing",
            case.name
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
