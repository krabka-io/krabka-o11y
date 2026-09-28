use futures::TryStreamExt as _;

use super::{
    AtomicU64, Bytes, ConditionalUpdateRequirement, GetOptions, GetRange, ObjectStore,
    ObjectStoreCapabilities, ObjectStoreError, ObjectStoreExt, ObjectStoreSemanticsError, Ordering,
    Path, PutMode, PutPayload, PutResult, SystemTime, UNIX_EPOCH, UpdateVersion, instrument,
};

/// The directory below a role's prefix that holds probe objects.
///
/// No signal lists this name. The logs retention sweep lists only `tenant=`
/// children, and every other sweep lists a block prefix of its own.
pub const OBJECT_STORE_PROBE_PREFIX: &str = ".krabka-probe";

/// The bytes of a probe object. Ten distinct bytes make a wrong range visible.
const PROBE_PAYLOAD: &[u8] = b"0123456789";

/// A key no other probe, in this process or another, writes.
fn probe_key(prefix: &Path) -> Path {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    prefix
        .clone()
        .join(OBJECT_STORE_PROBE_PREFIX)
        .join(format!("{nanos:x}-{:x}-{sequence}", std::process::id()))
}

/// Checks that `store` has the object-store semantics Krabka writes against,
/// before a role serves or consumes anything.
///
/// The probe writes one object below `prefix/.krabka-probe/` and deletes it
/// afterwards, whether the probe passes or fails. It checks five things, in
/// this order:
///
/// 1. **Create-if-absent.** A create succeeds, and a second create to the same
///    key fails with `AlreadyExists`. Index snapshots and backups claim a key
///    this way, and a store that ignores it lets two writers replace each
///    other.
/// 2. **Bounded ranged read.** A read of bytes 2 to 6 returns those bytes.
///    Every Parquet column read is a ranged read. A suffix read is also tried,
///    and its result goes in [`ObjectStoreCapabilities::suffix_range_read`].
/// 3. **List-after-write.** A listing straight after the write names the
///    object. Index loads and orphan sweeps decide from a listing.
/// 4. **Conditional update.** An update conditional on the current `ETag` or
///    version succeeds, and one conditional on the stale version fails with
///    `Precondition`. The probe sends both the `ETag` and the version, so GCS,
///    which matches on its generation, passes. `requirement` says whether a
///    store that does not implement the write fails the probe.
/// 5. **Idempotent delete.** A delete succeeds, and a second delete returns
///    success or `NotFound`.
///
/// # Errors
/// Returns the [`ObjectStoreSemanticsError`] variant that names the first
/// missing semantic, or [`ObjectStoreSemanticsError::ObjectStore`] when a
/// request fails for another reason, such as a bad credential.
#[instrument(level = "info", skip_all, fields(store = %store, prefix = %prefix), err)]
pub async fn verify_object_store_semantics(
    store: &dyn ObjectStore,
    prefix: &Path,
    requirement: ConditionalUpdateRequirement,
) -> Result<ObjectStoreCapabilities, ObjectStoreSemanticsError> {
    let key = probe_key(prefix);
    let created = create_if_absent(store, &key).await?;
    let outcome = probe_created_object(store, prefix, &key, created, requirement).await;
    if outcome.is_err() {
        // Best effort: the error being returned is the one worth reporting.
        drop(store.delete(&key).await);
    }
    let capabilities = outcome?;
    tracing::info!(
        conditional_update = capabilities.conditional_update,
        suffix_range_read = capabilities.suffix_range_read,
        "object store semantics verified"
    );
    Ok(capabilities)
}

async fn create_if_absent(
    store: &dyn ObjectStore,
    key: &Path,
) -> Result<PutResult, ObjectStoreSemanticsError> {
    match store
        .put_opts(
            key,
            PutPayload::from_static(PROBE_PAYLOAD),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(created) => Ok(created),
        Err(ObjectStoreError::NotImplemented { .. }) => {
            Err(ObjectStoreSemanticsError::CreateIfAbsentUnsupported {
                store: store.to_string(),
            })
        }
        Err(source) => Err(ObjectStoreSemanticsError::ObjectStore {
            step: "create a probe object",
            source,
        }),
    }
}

async fn probe_created_object(
    store: &dyn ObjectStore,
    prefix: &Path,
    key: &Path,
    created: PutResult,
    requirement: ConditionalUpdateRequirement,
) -> Result<ObjectStoreCapabilities, ObjectStoreSemanticsError> {
    match store
        .put_opts(
            key,
            PutPayload::from_static(b"second"),
            PutMode::Create.into(),
        )
        .await
    {
        Err(ObjectStoreError::AlreadyExists { .. }) => {}
        Ok(_) => {
            return Err(ObjectStoreSemanticsError::CreateIfAbsentIgnored {
                store: store.to_string(),
            });
        }
        Err(source) => {
            return Err(ObjectStoreSemanticsError::ObjectStore {
                step: "repeat a create-if-absent write",
                source,
            });
        }
    }

    let bounded = store.get_range(key, 2..6).await.map_err(|source| {
        ObjectStoreSemanticsError::ObjectStore {
            step: "read a byte range",
            source,
        }
    })?;
    if bounded != PROBE_PAYLOAD[2..6] {
        return Err(ObjectStoreSemanticsError::RangedReadMismatch {
            store: store.to_string(),
            range: "bounded",
        });
    }
    let suffix_range_read = suffix_read(store, key).await?;

    let listed = store
        .list(Some(&prefix.clone().join(OBJECT_STORE_PROBE_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|source| ObjectStoreSemanticsError::ObjectStore {
            step: "list the probe prefix",
            source,
        })?;
    if !listed.iter().any(|meta| &meta.location == key) {
        return Err(ObjectStoreSemanticsError::ListingMissedWrite {
            store: store.to_string(),
        });
    }

    let conditional_update = conditional_update(store, key, created, requirement).await?;
    idempotent_delete(store, key).await?;
    Ok(ObjectStoreCapabilities {
        conditional_update,
        suffix_range_read,
    })
}

async fn suffix_read(
    store: &dyn ObjectStore,
    key: &Path,
) -> Result<bool, ObjectStoreSemanticsError> {
    let options = GetOptions {
        range: Some(GetRange::Suffix(3)),
        ..GetOptions::default()
    };
    let read = match store.get_opts(key, options).await {
        Ok(result) => result.bytes().await,
        Err(ObjectStoreError::NotSupported { .. }) => return Ok(false),
        Err(error) => Err(error),
    };
    let bytes: Bytes = read.map_err(|source| ObjectStoreSemanticsError::ObjectStore {
        step: "read a suffix range",
        source,
    })?;
    if bytes != PROBE_PAYLOAD[PROBE_PAYLOAD.len() - 3..] {
        return Err(ObjectStoreSemanticsError::RangedReadMismatch {
            store: store.to_string(),
            range: "suffix",
        });
    }
    Ok(true)
}

async fn conditional_update(
    store: &dyn ObjectStore,
    key: &Path,
    created: PutResult,
    requirement: ConditionalUpdateRequirement,
) -> Result<bool, ObjectStoreSemanticsError> {
    let stale = UpdateVersion::from(created);
    match store
        .put_opts(
            key,
            PutPayload::from_static(b"updated"),
            PutMode::Update(stale.clone()).into(),
        )
        .await
    {
        Ok(_) => {}
        Err(ObjectStoreError::NotImplemented { .. }) => {
            return match requirement {
                ConditionalUpdateRequirement::Optional => Ok(false),
                ConditionalUpdateRequirement::Required => {
                    Err(ObjectStoreSemanticsError::ConditionalUpdateUnsupported {
                        store: store.to_string(),
                    })
                }
            };
        }
        Err(source) => {
            return Err(ObjectStoreSemanticsError::ObjectStore {
                step: "update the probe object conditionally",
                source,
            });
        }
    }
    match store
        .put_opts(
            key,
            PutPayload::from_static(b"stale"),
            PutMode::Update(stale).into(),
        )
        .await
    {
        Err(ObjectStoreError::Precondition { .. }) => Ok(true),
        Ok(_) => Err(ObjectStoreSemanticsError::ConditionalUpdateIgnored {
            store: store.to_string(),
        }),
        Err(source) => Err(ObjectStoreSemanticsError::ObjectStore {
            step: "repeat a stale conditional update",
            source,
        }),
    }
}

async fn idempotent_delete(
    store: &dyn ObjectStore,
    key: &Path,
) -> Result<(), ObjectStoreSemanticsError> {
    store
        .delete(key)
        .await
        .map_err(|source| ObjectStoreSemanticsError::ObjectStore {
            step: "delete the probe object",
            source,
        })?;
    match store.delete(key).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(source) => Err(ObjectStoreSemanticsError::DeleteNotIdempotent {
            store: store.to_string(),
            source,
        }),
    }
}
