use super::{BlockSkipReason, ObjectStoreError, ParquetError};

/// Why one named block could not be read.
///
/// The backend error is kept whole rather than flattened to a string, which is
/// what [`BlockStoreError::ObjectStore`](super::BlockStoreError::ObjectStore)
/// does. A caller that has to tell "this one object is gone" from "the store
/// is down" needs to reach `object_store::Error::NotFound`, and a string does
/// not let it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlockReadFailure {
    /// The object store refused or could not serve the object.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),

    /// The bytes are there but are not a readable Parquet block.
    #[error(transparent)]
    Parquet(#[from] ParquetError),
}

impl BlockReadFailure {
    /// The backend error behind this failure, when there is one.
    ///
    /// A store error raised *during* Parquet decoding arrives wrapped in
    /// [`ParquetError::External`], because that is the only shape the Parquet
    /// reader has for an error from the byte source it was handed. Unwrapping
    /// it here is what lets a caller match `NotFound` no matter which layer
    /// noticed the object had gone.
    #[must_use]
    pub fn object_store(&self) -> Option<&ObjectStoreError> {
        match self {
            Self::ObjectStore(error) => Some(error),
            Self::Parquet(ParquetError::External(external)) => external.downcast_ref(),
            Self::Parquet(_) => None,
        }
    }

    /// Whether the object itself is absent.
    #[must_use]
    pub fn is_missing(&self) -> bool {
        matches!(self.object_store(), Some(ObjectStoreError::NotFound { .. }))
    }

    /// How a scan should describe leaving this block out, or `None` when the
    /// failure is not the block's fault and no scan may skip it.
    ///
    /// This is the whole "degraded, and here is the warning" versus "the store
    /// is down" decision, in one place:
    ///
    /// - A missing object is one block's problem. Compaction replaces blocks
    ///   without deleting the inputs it replaced, and an index snapshot
    ///   restored from an older generation still names the replaced keys, so
    ///   this happens in ordinary operation and not only under corruption.
    /// - Undecodable bytes are one block's problem too, and the reader has
    ///   already proved it by failing on them.
    /// - Anything else — a timeout, a 5xx, a refused credential — says nothing
    ///   about this block. Skipping on those would turn a broken store into a
    ///   confident, quietly empty answer, so they propagate.
    #[must_use]
    pub fn skip_reason(&self) -> Option<BlockSkipReason> {
        match self.object_store() {
            Some(ObjectStoreError::NotFound { .. }) => Some(BlockSkipReason::Missing),
            Some(_) => None,
            // Only a Parquet decode failure gets here: a `Self::ObjectStore`
            // failure always yields its own error above.
            None => Some(BlockSkipReason::Corrupt),
        }
    }
}
