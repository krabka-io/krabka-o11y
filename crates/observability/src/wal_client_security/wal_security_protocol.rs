use derive_more::Display;

use super::{ListenerProtocol, ValueEnum};

/// The protocol a write-ahead log connection speaks, as Kafka's
/// `security.protocol` names it.
///
/// The flag accepts the names in any letter case, as the Kafka clients do.
#[derive(Clone, Copy, Debug, Default, Display, Eq, Hash, PartialEq, ValueEnum)]
pub enum WalSecurityProtocol {
    /// Plain TCP, with no TLS and no SASL.
    #[default]
    #[value(name = "PLAINTEXT")]
    #[display("PLAINTEXT")]
    Plaintext,
    /// TLS, with an optional client certificate for mutual TLS.
    #[value(name = "SSL")]
    #[display("SSL")]
    Ssl,
    /// SASL authentication over plain TCP.
    #[value(name = "SASL_PLAINTEXT")]
    #[display("SASL_PLAINTEXT")]
    SaslPlaintext,
    /// SASL authentication over TLS.
    #[value(name = "SASL_SSL")]
    #[display("SASL_SSL")]
    SaslSsl,
}

impl WalSecurityProtocol {
    /// Whether this protocol wraps the connection in TLS.
    #[must_use]
    pub fn requires_tls(self) -> bool {
        ListenerProtocol::from(self).requires_tls()
    }

    /// Whether this protocol authenticates the connection with SASL.
    #[must_use]
    pub fn requires_sasl(self) -> bool {
        ListenerProtocol::from(self).requires_sasl()
    }
}

impl From<WalSecurityProtocol> for ListenerProtocol {
    fn from(protocol: WalSecurityProtocol) -> Self {
        match protocol {
            WalSecurityProtocol::Plaintext => Self::Plaintext,
            WalSecurityProtocol::Ssl => Self::Ssl,
            WalSecurityProtocol::SaslPlaintext => Self::SaslPlaintext,
            WalSecurityProtocol::SaslSsl => Self::SaslSsl,
        }
    }
}
