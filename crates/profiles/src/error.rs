//! Crate-wide error + ingest-edge HTTP status mapping.

use krabka_blockstore::TenantResolveError;
use krabka_observability::server_security::TenantDenied;

/// Errors across the profiles ingest pipeline.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ProfilesError {
    #[error("unsupported content-type/format: {0}")]
    UnsupportedFormat(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("gunzip failed: {0}")]
    Gunzip(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The `X-Scope-OrgID` header names a tenant that is not a valid tenant id.
    ///
    /// The message is the [`TenantResolveError`] text alone, which is the text
    /// that Grafana's `dskit` sends for the same header.
    #[error(transparent)]
    Tenant(#[from] TenantResolveError),
    /// The principal of the request may not use the tenant that the request names.
    ///
    /// The message is the [`TenantDenied`] text alone. A plain HTTP door
    /// answers with the 403 of [`TenantDenied`] itself.
    #[error(transparent)]
    TenantDenied(#[from] TenantDenied),
    #[error("{0}")]
    Limit(crate::limits::LimitError),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("wal codec: {0}")]
    Wal(String),
    #[error("produce failed: {0}")]
    Produce(String),
    /// A WAL batch that appended in part or not at all.
    ///
    /// This is separate from [`Self::Produce`] because it carries how much of
    /// the batch reached the broker. The status stays 500, so a Pyroscope or
    /// Alloy client retries the whole request. Profiles have no query-time
    /// deduplication, so that retry writes what already landed a second time.
    #[error("wal append wrote {appended} of {total} records: {message}")]
    ProduceBatch {
        appended: usize,
        total: usize,
        message: String,
    },
    #[error("block build failed: {0}")]
    Block(String),
    #[error("pprof: {0}")]
    Pprof(String),
    /// An unexpected server-side fault, for example a poisoned lock. The inner
    /// string is for server-side logging only. Callers must NOT show it verbatim
    /// to clients. The ingest edge maps this variant to a generic 500 message.
    #[error("internal error: {0}")]
    Internal(String),
}

impl ProfilesError {
    /// Map to the ingest-edge HTTP status.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedFormat(_) => 415,
            // `Tenant` is here for Krabka's own reason. Pyroscope with
            // multi-tenancy off does not read `X-Scope-OrgID`, so it has no
            // rejection to match. Krabka isolates WAL records, limits and
            // blocks by tenant, so it rejects a malformed name. A 400 is a
            // client fault, so the Connect doors send `invalid_argument` with
            // the same message.
            Self::Decode(_)
            | Self::Gunzip(_)
            | Self::Invalid(_)
            | Self::Tenant(_)
            | Self::Pprof(_)
            | Self::TooLarge { .. } => 400,
            // Only a service with a credentials file can deny a tenant. The
            // Connect doors send `permission_denied`, which is also a 403.
            Self::TenantDenied(_) => 403,
            Self::Limit(err) => err.http_status(),
            Self::Wal(_)
            | Self::Produce(_)
            | Self::ProduceBatch { .. }
            | Self::Block(_)
            | Self::Internal(_) => 500,
        }
    }
}

impl From<krabka_pprof::ProfileError> for ProfilesError {
    fn from(err: krabka_pprof::ProfileError) -> Self {
        Self::Pprof(err.to_string())
    }
}

impl From<krabka_observability::wal_produce::WalBatchError<ProfilesError>> for ProfilesError {
    fn from(error: krabka_observability::wal_produce::WalBatchError<ProfilesError>) -> Self {
        let (appended, total) = (error.appended(), error.total());
        Self::ProduceBatch {
            appended,
            total,
            message: error.into_source().to_string(),
        }
    }
}

impl From<crate::limits::LimitError> for ProfilesError {
    fn from(err: crate::limits::LimitError) -> Self {
        Self::Limit(err)
    }
}
