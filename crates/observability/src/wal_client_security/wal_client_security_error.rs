use super::{Error, PathBuf, WalSaslMechanism, WalSecurityProtocol, io};

/// Why a set of write-ahead log security flags gives no usable policy.
///
/// Each variant names the flag at fault. A flag that the selected protocol or
/// mechanism does not use is an error and not a warning: a credential that
/// the process silently ignores is worse than a refusal. No variant holds the
/// contents of a file.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WalClientSecurityError {
    /// A TLS flag is set, but the protocol does not use TLS.
    #[error("{flag} needs --wal-security-protocol SSL or SASL_SSL, but the protocol is {protocol}")]
    TlsFlagWithoutTlsProtocol {
        flag: &'static str,
        protocol: WalSecurityProtocol,
    },
    /// A TLS protocol has no CA file, so the client could verify no broker.
    #[error("--wal-security-protocol {protocol} needs --wal-tls-ca-path")]
    MissingTlsCaPath { protocol: WalSecurityProtocol },
    #[error("--wal-security-protocol {protocol} needs --wal-tls-server-name")]
    MissingTlsServerName { protocol: WalSecurityProtocol },
    #[error("--wal-tls-cert-path needs --wal-tls-key-path")]
    TlsCertWithoutKey,
    #[error("--wal-tls-key-path needs --wal-tls-cert-path")]
    TlsKeyWithoutCert,
    /// A SASL flag is set, but the protocol does not use SASL.
    #[error(
        "{flag} needs --wal-security-protocol SASL_PLAINTEXT or SASL_SSL, but the protocol is {protocol}"
    )]
    SaslFlagWithoutSaslProtocol {
        flag: &'static str,
        protocol: WalSecurityProtocol,
    },
    #[error("--wal-security-protocol {protocol} needs --wal-sasl-mechanism")]
    MissingSaslMechanism { protocol: WalSecurityProtocol },
    /// See [`WalSaslMechanism::Gssapi`].
    #[error(
        "--wal-sasl-mechanism GSSAPI is not supported yet; use PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or OAUTHBEARER"
    )]
    GssapiUnsupported,
    #[error("--wal-sasl-mechanism {mechanism} needs --wal-sasl-username")]
    MissingSaslUsername { mechanism: WalSaslMechanism },
    #[error("--wal-sasl-mechanism {mechanism} needs --wal-sasl-password-path")]
    MissingSaslPasswordPath { mechanism: WalSaslMechanism },
    #[error("--wal-sasl-mechanism OAUTHBEARER needs --wal-sasl-oauthbearer-token-path")]
    MissingOAuthBearerTokenPath,
    /// A SASL flag is set, but the selected mechanism does not use it.
    #[error("--wal-sasl-mechanism {mechanism} does not use {flag}")]
    SaslFlagNotUsedByMechanism {
        flag: &'static str,
        mechanism: WalSaslMechanism,
    },
    /// The process cannot read a file that a flag names.
    #[error("cannot read the {flag} file {}: {source}", path.display())]
    UnreadableFile {
        flag: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    #[error("the --wal-sasl-password-path file {} is not UTF-8", path.display())]
    PasswordFileNotUtf8 { path: PathBuf },
    /// The password file holds nothing but line breaks.
    #[error("the --wal-sasl-password-path file {} is empty", path.display())]
    EmptyPasswordFile { path: PathBuf },
}
