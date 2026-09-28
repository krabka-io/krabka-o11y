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
