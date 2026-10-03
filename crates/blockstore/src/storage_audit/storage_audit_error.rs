/// Errors that stop a storage audit or a repair pass.
///
/// A damaged object is not one of them: the audit reports it as a
/// [`StorageFinding`](super::StorageFinding) and carries on. Only a failure
/// that leaves the pass unable to tell what exists reaches here.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageAuditError {
    /// Listing or reading the store failed, so the audit does not know what
    /// the store holds.
    #[error("object store error: {0}")]
    ObjectStore(String),
    /// A reader failed for a reason that is not the fault of one object.
    #[error("block store error: {0}")]
    BlockStore(String),
    /// The requested scope is not one a repair accepts.
    #[error("invalid repair scope: {0}")]
    InvalidScope(String),
    /// A report was written by a build that uses another schema version.
    #[error("unsupported storage audit schema version {actual}; expected {expected}")]
    UnsupportedSchemaVersion { actual: u32, expected: u32 },
    /// A report or a log line does not decode.
    #[error("invalid storage audit report: {0}")]
    InvalidReport(String),
    /// Writing the repair audit log failed.
    #[error("audit log write failed: {0}")]
    AuditLog(String),
}

impl From<object_store::Error> for StorageAuditError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}
