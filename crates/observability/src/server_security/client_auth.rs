use clap::ValueEnum;

/// Whether a TLS listener asks a client for a certificate.
///
/// The values are the Go `tls.ClientAuthType` names that dskit's
/// `-server.http-tls-client-auth` flag accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum ClientAuth {
    /// The listener does not ask for a client certificate.
    #[default]
    #[value(name = "NoClientCert")]
    NoClientCert,
    /// The listener asks for a client certificate, and a client without one
    /// still connects.
    ///
    /// This differs from Go on purpose. Go does not verify a certificate under
    /// `RequestClientCert`. Krabka verifies every certificate that a client
    /// sends against the client CA, and ends the handshake if it does not
    /// verify. An unverified certificate is not an identity, so this mode
    /// needs `--server-tls-client-ca-path`.
    #[value(name = "RequestClientCert")]
    RequestClientCert,
    /// The listener ends the handshake unless the client sends a certificate
    /// that verifies against the client CA.
    #[value(name = "RequireAndVerifyClientCert")]
    RequireAndVerifyClientCert,
}

impl ClientAuth {
    /// The flag value that selects this mode.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoClientCert => "NoClientCert",
            Self::RequestClientCert => "RequestClientCert",
            Self::RequireAndVerifyClientCert => "RequireAndVerifyClientCert",
        }
    }
}
