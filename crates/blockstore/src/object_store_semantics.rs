//! A startup probe for the object-store semantics Krabka depends on.
//!
//! Every role that writes blocks calls [`verify_object_store_semantics`] on its
//! store before it serves or consumes anything. A role that only reads calls
//! [`verify_object_store_read_access`], which writes nothing, so it can run
//! with a read-only credential. [`verify_object_store_access`] picks one of the
//! two by [`ObjectStoreAccess`]. A store that lacks a semantic then stops the role with an error
//! that names it, before any data is accepted. Without the probe, the same
//! store starts and fails later, on the first sweep or the first concurrent
//! writer, with an error far from its cause or with no error at all.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMode, PutPayload, PutResult, UpdateVersion, path::Path,
};
use tracing::instrument;
use url::Url;

/// Runs the startup probe that fits `access`.
///
/// [`ObjectStoreAccess::ReadWrite`] runs [`verify_object_store_semantics`]
/// with `requirement`. [`ObjectStoreAccess::ReadOnly`] runs
/// [`verify_object_store_read_access`], which writes nothing, and ignores
/// `requirement`.
///
/// # Errors
/// Returns the error of the probe that ran.
pub async fn verify_object_store_access(
    store: &dyn ObjectStore,
    prefix: &Path,
    access: ObjectStoreAccess,
    requirement: ConditionalUpdateRequirement,
) -> Result<(), ObjectStoreSemanticsError> {
    match access {
        ObjectStoreAccess::ReadWrite => verify_object_store_semantics(store, prefix, requirement)
            .await
            .map(drop),
        ObjectStoreAccess::ReadOnly => verify_object_store_read_access(store, prefix).await,
    }
}

use futures::StreamExt as _;

/// The most bytes the read probe reads from an object.
const READ_PROBE_BYTES: u64 = 4;

/// Checks that a read-only role can list and read `store` below `prefix`,
/// and writes nothing.
///
/// The probe lists `prefix` and takes the first object that is not a probe
/// object. When there is one, it reads the first bytes of that object, up to
/// four, as a bounded range and checks that it gets that many bytes. An empty
/// prefix passes on the listing alone, because a new deployment has no
/// objects yet. An object that is deleted between the listing and the read
/// also passes, because a compactor can remove a block at any time.
///
/// The probe needs only list and get, so it passes on a read-only credential.
/// It does not check create-if-absent, list-after-write, conditional update or
/// delete, because they need a write. The roles that write run
/// [`verify_object_store_semantics`](super::verify_object_store_semantics).
///
/// # Errors
/// Returns [`ObjectStoreSemanticsError::RangedReadMismatch`] when the range
/// read returns the wrong number of bytes, or
/// [`ObjectStoreSemanticsError::ObjectStore`] when the listing or the read
/// fails, such as for a bad credential or a missing bucket.
#[instrument(level = "info", skip_all, fields(store = %store, prefix = %prefix), err)]
pub async fn verify_object_store_read_access(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<(), ObjectStoreSemanticsError> {
    let Some(sample) = first_object(store, prefix).await? else {
        tracing::info!("object store read access verified on an empty prefix");
        return Ok(());
    };
    let end = sample.size.min(READ_PROBE_BYTES);
    match store.get_range(&sample.location, 0..end).await {
        Ok(bytes) if bytes.len() as u64 == end => {}
        Ok(_) => {
            return Err(ObjectStoreSemanticsError::RangedReadMismatch {
                store: store.to_string(),
                range: "bounded",
            });
        }
        Err(ObjectStoreError::NotFound { .. }) => {}
        Err(source) => {
            return Err(ObjectStoreSemanticsError::ObjectStore {
                step: "read a byte range of a listed object",
                source,
            });
        }
    }
    tracing::info!(sample = %sample.location, "object store read access verified");
    Ok(())
}

/// The first listed object below `prefix` that is not a probe object.
async fn first_object(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<Option<ObjectMeta>, ObjectStoreSemanticsError> {
    let probes = prefix.clone().join(OBJECT_STORE_PROBE_PREFIX);
    let mut listing = store.list(Some(prefix));
    while let Some(entry) = listing.next().await {
        let meta = entry.map_err(|source| ObjectStoreSemanticsError::ObjectStore {
            step: "list the configured prefix",
            source,
        })?;
        if !meta.location.prefix_matches(&probes) && meta.size > 0 {
            return Ok(Some(meta));
        }
    }
    Ok(None)
}

use futures::TryStreamExt as _;

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
/// afterwards, whether the probe passes or fails. A create that fails can
/// still have written the object, for example when the connection drops after
/// the store commits it, so the probe also deletes the key then. It does not
/// delete the key after `AlreadyExists`, because then the object belongs to
/// another writer. It checks five things, in this order:
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
    let outcome = match store
        .put_opts(
            &key,
            PutPayload::from_static(PROBE_PAYLOAD),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(created) => probe_created_object(store, prefix, &key, created, requirement).await,
        // The store refused the create, so the probe owns no object to delete.
        Err(
            error @ (ObjectStoreError::AlreadyExists { .. }
            | ObjectStoreError::NotImplemented { .. }),
        ) => return Err(create_error(store, error)),
        Err(error) => Err(create_error(store, error)),
    };
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

fn create_error(store: &dyn ObjectStore, error: ObjectStoreError) -> ObjectStoreSemanticsError {
    match error {
        ObjectStoreError::NotImplemented { .. } => {
            ObjectStoreSemanticsError::CreateIfAbsentUnsupported {
                store: store.to_string(),
            }
        }
        source => ObjectStoreSemanticsError::ObjectStore {
            step: "create a probe object",
            source,
        },
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

/// Whether a store has to honour a conditional update for Krabka to run on it.
///
/// Krabka writes create-if-absent on every backend. It writes a conditional
/// update, which is `If-Match` on an `ETag` or a GCS generation, to reclaim
/// superseded index payloads and to swap profiles recording rules. A local
/// filesystem cannot do that write, and a single process that owns its
/// directory does not need it. A shared bucket does: without it, two writers
/// can each replace the other's object and neither is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionalUpdateRequirement {
    /// The probe fails when the store does not implement a conditional update.
    Required,
    /// The probe reports a missing conditional update and does not fail.
    Optional,
}

impl ConditionalUpdateRequirement {
    /// The requirement for the store that `url` configures.
    ///
    /// `file://`, `memory://` and a bare relative path are local to one
    /// process, so a conditional update is optional there. Every other scheme
    /// names a store that more than one process can reach, so it is required.
    #[must_use]
    pub fn for_object_store_url(url: &str) -> Self {
        match Url::parse(url) {
            Ok(parsed) if matches!(parsed.scheme(), "file" | "memory") => Self::Optional,
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::Optional,
            _ => Self::Required,
        }
    }
}

/// What a role does with its object store, and so how much of it the startup
/// probe may exercise.
///
/// A role that only reads, such as a querier, can run with a credential that
/// has no put or delete. The write probe would fail on that credential, and
/// it would test semantics that the role never uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectStoreAccess {
    /// The role writes and deletes objects. The probe checks every semantic
    /// with [`verify_object_store_semantics`](verify_object_store_semantics).
    ReadWrite,
    /// The role only lists and reads. The probe writes nothing, and checks
    /// what [`verify_object_store_read_access`](super::verify_object_store_read_access)
    /// checks.
    ReadOnly,
}

/// What [`verify_object_store_semantics`](verify_object_store_semantics)
/// found that a store can do, beyond what it requires.
///
/// Create-if-absent, a bounded ranged read, list-after-write and an idempotent
/// delete are not fields. The probe fails when any one of them is missing, so a
/// value of this type means the store has all four.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectStoreCapabilities {
    /// A write conditional on the current `ETag` or version succeeds, and one
    /// conditional on a stale `ETag` or version fails with a precondition error.
    pub conditional_update: bool,
    /// A read of the last `n` bytes works without the object's size. Azure
    /// Blob Storage refuses it, and the block reader then reads the size first.
    pub suffix_range_read: bool,
}

/// A store that does not have a semantic Krabka depends on.
///
/// Each variant names the missing semantic and the store, so the message that
/// stops a role at startup says what to change. See
/// `docs/object_store_contract.md` for the settings that enable each one.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ObjectStoreSemanticsError {
    /// The store refused a create-if-absent write as not implemented.
    #[error(
        "object store {store} does not implement create-if-absent writes; \
         enable conditional puts (for S3, remove AWS_CONDITIONAL_PUT=disabled)"
    )]
    CreateIfAbsentUnsupported {
        /// The store, as it describes itself.
        store: String,
    },
    /// A second create-if-absent write to the same key succeeded.
    #[error(
        "object store {store} accepted a second create-if-absent write to one key; \
         it ignores If-None-Match, and concurrent writers would replace each other"
    )]
    CreateIfAbsentIgnored {
        /// The store, as it describes itself.
        store: String,
    },
    /// The store refused a conditional update as not implemented.
    #[error(
        "object store {store} does not implement conditional updates; \
         enable conditional puts (for S3, remove AWS_CONDITIONAL_PUT=disabled)"
    )]
    ConditionalUpdateUnsupported {
        /// The store, as it describes itself.
        store: String,
    },
    /// An update conditional on a stale `ETag` or version succeeded.
    #[error(
        "object store {store} accepted an update conditional on a stale version; \
         it ignores If-Match, and a sweep could overwrite a restored object"
    )]
    ConditionalUpdateIgnored {
        /// The store, as it describes itself.
        store: String,
    },
    /// A ranged read returned bytes other than the ones asked for.
    #[error("object store {store} returned the wrong bytes for a {range} read")]
    RangedReadMismatch {
        /// The store, as it describes itself.
        store: String,
        /// The kind of range that failed.
        range: &'static str,
    },
    /// A listing straight after a write did not name the written object.
    #[error(
        "object store {store} did not list an object that it had just acknowledged; \
         the orphan sweep and index loads need list-after-write consistency"
    )]
    ListingMissedWrite {
        /// The store, as it describes itself.
        store: String,
    },
    /// A second delete of one key failed with something other than not-found.
    #[error("object store {store} failed a repeated delete of one key: {source}")]
    DeleteNotIdempotent {
        /// The store, as it describes itself.
        store: String,
        /// What the second delete returned.
        #[source]
        source: object_store::Error,
    },
    /// A probe request failed for a reason that says nothing about semantics:
    /// a credential, a missing bucket, or an unreachable endpoint.
    #[error("object store semantics probe could not {step}: {source}")]
    ObjectStore {
        /// The probe step that failed.
        step: &'static str,
        /// The error the store returned.
        #[source]
        source: object_store::Error,
    },
}
