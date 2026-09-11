use std::{io, path::PathBuf};

use super::{ClientAuth, CredentialsError};

/// Why the security flags do not give a usable [`ServerSecurity`](super::ServerSecurity).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerSecurityError {
    #[error("--server-tls-cert-path is set, but --server-tls-key-path is not")]
    CertificateWithoutKey,

    #[error("--server-tls-key-path is set, but --server-tls-cert-path is not")]
    KeyWithoutCertificate,

    #[error("--server-tls-client-ca-path is set, but --server-tls-cert-path is not")]
    ClientCaWithoutCertificate,

    #[error("--server-tls-client-auth={} needs --server-tls-client-ca-path", .client_auth.as_str())]
    ClientAuthWithoutClientCa { client_auth: ClientAuth },

    /// A client CA with `NoClientCert` would never be used, which is almost
    /// always a mistake in the flags.
    #[error(
        "--server-tls-client-ca-path is set, but --server-tls-client-auth is NoClientCert, so the listener would never ask for a client certificate"
    )]
    ClientCaWithoutClientAuth,

    #[error("--server-tls-client-auth={} needs --server-tls-cert-path", .client_auth.as_str())]
    ClientAuthWithoutCertificate { client_auth: ClientAuth },

    #[error("--internal-client-tls-cert-path is set, but --internal-client-tls-key-path is not")]
    InternalClientCertificateWithoutKey,

    #[error("--internal-client-tls-key-path is set, but --internal-client-tls-cert-path is not")]
    InternalClientKeyWithoutCertificate,

    #[error("cannot read {}: {source}", .path.display())]
    ReadFile { path: PathBuf, source: io::Error },

    #[error("{}: not valid PEM: {reason}", .path.display())]
    InvalidPem { path: PathBuf, reason: String },

    #[error("{}: holds no PEM certificate", .path.display())]
    NoCertificate { path: PathBuf },

    #[error("{}: holds no PEM private key", .path.display())]
    NoPrivateKey { path: PathBuf },

    /// rustls refused the certificate chain, the key, or the pair of them.
    #[error("{} and {}: {source}", .certificate.display(), .key.display())]
    InvalidServerCertificate {
        certificate: PathBuf,
        key: PathBuf,
        source: rustls::Error,
    },

    #[error("{}: not a usable client CA: {reason}", .path.display())]
    InvalidClientCa { path: PathBuf, reason: String },

    #[error("credentials file {}: {source}", .path.display())]
    Credentials {
        path: PathBuf,
        source: CredentialsError,
    },

    #[error("{}: the internal client token is empty or is not a valid header value", .path.display())]
    InvalidInternalClientToken { path: PathBuf },

    #[error("{} and {}: not a usable client identity: {source}", .certificate.display(), .key.display())]
    InvalidInternalClientIdentity {
        certificate: PathBuf,
        key: PathBuf,
        source: reqwest::Error,
    },

    #[error("{}: not a usable CA bundle: {source}", .path.display())]
    InvalidInternalClientCa {
        path: PathBuf,
        source: reqwest::Error,
    },

    #[error("cannot read the listener's local address: {0}")]
    LocalAddr(io::Error),
}
