use super::{BlockReadFailure, BlockSkipReason, ParquetError, SkippedBlock};

/// Errors raised by the block store. Backend errors are stringified so public
/// errors stay stable across dependency details, except where a caller's whole
/// job is to inspect them: [`Self::BlockUnreadable`], which says whether a
/// scan may leave one block out, and [`Self::Parquet`], which says whether a
/// failed write is worth attempting again.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlockStoreError {
    /// One named block could not be read, and the failure is that block's
    /// alone: the object is missing, or its bytes are not a Parquet block.
    ///
    /// Separate from [`Self::ObjectStore`] because those two call for opposite
    /// responses. A store that is down fails the query. A single unreadable
    /// block can be skipped, if — and only if — the caller says so in the
    /// answer it returns. [`Self::skipped_block`] turns this into the report
    /// entry that says so.
    #[error("block `{object_key}` is unreadable: {source}")]
    BlockUnreadable {
        /// The object key that could not be read.
        object_key: String,
        /// The backend error, kept whole so the caller can match on it.
        #[source]
        source: Box<BlockReadFailure>,
    },

    #[error("object store error: {0}")]
    ObjectStore(String),

    /// The Parquet layer failed.
    ///
    /// The error is kept whole, not stringified, for the same reason
    /// [`Self::BlockUnreadable`] keeps its source: a *write* that fails
    /// because the object store went away arrives here and nowhere else. The
    /// block is streamed through a `BufWriter`, so the Parquet writer is what
    /// notices, and it reports an [`External`](ParquetError::External) error
    /// wrapping the I/O error wrapping the backend error. A caller deciding
    /// whether to retry has to be able to reach that, and a string does not
    /// let it. See
    /// [`transient_object_store_error`](crate::transient_object_store_error).
    #[error("parquet error: {0}")]
    Parquet(#[source] Box<ParquetError>),

    #[error("datafusion error: {0}")]
    DataFusion(String),

    #[error("invalid block: {0}")]
    InvalidBlock(String),

    /// A profile series was offered to the profile index without the label
    /// that carries its profile type.
    ///
    /// The profile type is not stored beside the series: a load recomputes it
    /// from this label. A series that lacks the label therefore has no type to
    /// be found under, now or after a reload, and a profile written under it
    /// would store without complaint and answer no query. Ingest's
    /// multi-value split sets the label on every series it emits, so a series
    /// without it is a fault in the writer, not in a client's payload.
    #[error(
        "profile series {{{labels}}} of tenant `{tenant}` (fingerprint {fingerprint}) \
         has no `{label}` label, so no profile-type selector could ever reach it"
    )]
    MissingProfileTypeLabel {
        /// The label the series must carry, so the message names it.
        label: &'static str,
        /// The tenant the series was offered for.
        tenant: String,
        /// The fingerprint the series would have been registered under.
        fingerprint: u64,
        /// The series' own labels, rendered `name="value"`, comma separated.
        labels: String,
    },

    #[error("index snapshot serialization error: {0}")]
    Serde(String),
}

impl BlockStoreError {
    /// Attributes `failure` to the block at `object_key`.
    pub fn block_unreadable(object_key: impl Into<String>, failure: BlockReadFailure) -> Self {
        Self::BlockUnreadable {
            object_key: object_key.into(),
            source: Box::new(failure),
        }
    }

    /// The block this error is about and the backend error behind it, when the
    /// error is about one block.
    #[must_use]
    pub fn unreadable_block(&self) -> Option<(&str, &BlockReadFailure)> {
        match self {
            Self::BlockUnreadable { object_key, source } => Some((object_key, source)),
            _ => None,
        }
    }

    /// The report entry for skipping this block, when a scan is allowed to
    /// skip it.
    ///
    /// `None` means the error is not one block's fault — the store is
    /// unreachable, or the request was rejected — and a scan that carried on
    /// would be answering from a store it cannot read. See
    /// [`BlockReadFailure::skip_reason`].
    #[must_use]
    pub fn skipped_block(&self) -> Option<SkippedBlock> {
        let (object_key, failure) = self.unreadable_block()?;
        Some(SkippedBlock {
            object_key: object_key.to_string(),
            reason: failure.skip_reason()?,
            detail: failure.to_string(),
        })
    }

    /// Whether this error says one named block is absent from the store.
    #[must_use]
    pub fn is_block_missing(&self) -> bool {
        self.unreadable_block()
            .is_some_and(|(_, failure)| failure.skip_reason() == Some(BlockSkipReason::Missing))
    }
}

impl From<object_store::Error> for BlockStoreError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}

impl From<ParquetError> for BlockStoreError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(Box::new(error))
    }
}

impl From<datafusion::error::DataFusionError> for BlockStoreError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(error.to_string())
    }
}

impl From<serde_json::Error> for BlockStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serde(error.to_string())
    }
}
