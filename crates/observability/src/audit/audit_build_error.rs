use std::path::PathBuf;

use krabka_client_producer::ProducerError;

use super::AuditError;

/// A reason that [`AuditService`](super::AuditService) could not start the
/// audit writer.
///
/// Each variant is an operator error or a broker that the service cannot
/// reach. The service should stop, because it cannot keep the audit trail
/// that the operator asked for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuditBuildError {
    /// `--audit-topic` is set, and neither `--audit-bootstrap` nor the service
    /// gives a bootstrap server.
    #[error("--audit-topic is set but no bootstrap server is: set --audit-bootstrap")]
    MissingBootstrap,

    /// Only one of `--audit-signing-key-path` and `--audit-signing-key-id` is
    /// set.
    #[error("--audit-signing-key-path and --audit-signing-key-id must be set together")]
    IncompleteSigningKey,

    /// The audit producer did not start.
    #[error("audit producer for {bootstrap}: {source}")]
    Producer {
        /// The bootstrap servers that the producer tried.
        bootstrap: String,
        /// The producer's error.
        #[source]
        source: ProducerError,
    },

    /// The audit spool did not open, or its contents did not read.
    #[error("audit spool in {}: {source}", .dir.display())]
    Spool {
        /// The spool directory.
        dir: PathBuf,
        /// The spool's error.
        #[source]
        source: AuditError,
    },

    /// The checkpoint signing key did not load.
    #[error("audit signing key {}: {source}", .path.display())]
    SigningKey {
        /// The key file.
        path: PathBuf,
        /// The key loader's error.
        #[source]
        source: AuditError,
    },
}
