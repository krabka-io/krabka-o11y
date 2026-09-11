use super::{SaslMechanism, ValueEnum, fmt};

/// The SASL mechanism a write-ahead log connection authenticates with, as
/// Kafka's `sasl.mechanism` names it.
///
/// The flag accepts the names in upper case only, as the Kafka clients do.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum WalSaslMechanism {
    /// SASL/PLAIN, with a user name and a password file.
    #[value(name = "PLAIN")]
    Plain,
    /// SASL/SCRAM-SHA-256, with a user name and a password file.
    #[value(name = "SCRAM-SHA-256")]
    ScramSha256,
    /// SASL/SCRAM-SHA-512, with a user name and a password file.
    #[value(name = "SCRAM-SHA-512")]
    ScramSha512,
    /// SASL/OAUTHBEARER, with a bearer token file.
    #[value(name = "OAUTHBEARER")]
    OAuthBearer,
    /// SASL/GSSAPI (Kerberos). The flag accepts it, and
    /// [`WalClientSecurityArgs::load`](super::WalClientSecurityArgs::load)
    /// refuses it.
    ///
    /// `krabka-client-core` supports GSSAPI, but a test of it needs a KDC, and
    /// no Krabka deployment asks for Kerberos yet. A clear refusal is better
    /// than a mechanism that nothing tests. `--help` does not list the value.
    #[value(name = "GSSAPI", hide = true)]
    Gssapi,
}

impl From<WalSaslMechanism> for SaslMechanism {
    fn from(mechanism: WalSaslMechanism) -> Self {
        match mechanism {
            WalSaslMechanism::Plain => Self::Plain,
            WalSaslMechanism::ScramSha256 => Self::ScramSha256,
            WalSaslMechanism::ScramSha512 => Self::ScramSha512,
            WalSaslMechanism::OAuthBearer => Self::OAuthBearer,
            WalSaslMechanism::Gssapi => Self::Gssapi,
        }
    }
}

impl fmt::Display for WalSaslMechanism {
    // The flag spelling, which is also the Kafka wire name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self
            .to_possible_value()
            .expect("no mechanism variant is skipped");
        formatter.write_str(value.get_name())
    }
}
