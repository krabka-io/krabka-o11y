use super::{
    AdminError, AuditBuildError, CompactionFrontierStoreError, CompactorRunError, ConsumerError,
    CriticalTaskError, Error, LogDeleteRequestStoreError, ProducerError, ServerSecurityError,
    ServiceConfigError, WalClientSecurityError,
};

#[derive(Debug, Error)]
pub enum ServiceRuntimeError {
    #[error(transparent)]
    Config(#[from] ServiceConfigError),
    #[error(transparent)]
    Admin(#[from] AdminError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error(transparent)]
    Compactor(#[from] CompactorRunError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    CriticalTask(#[from] CriticalTaskError),
    /// The TLS or authentication flags of the data port do not load.
    #[error(transparent)]
    ServerSecurity(#[from] ServerSecurityError),
    /// The TLS or SASL flags of the WAL connections do not load.
    #[error(transparent)]
    WalClientSecurity(#[from] WalClientSecurityError),
    /// The audit layer that the audit flags name does not start.
    #[error(transparent)]
    Audit(#[from] AuditBuildError),
}
