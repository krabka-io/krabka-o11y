use std::path::PathBuf;

use clap::Args;
use krabka_units::Time;

use super::{
    ClientAuth, ServerSecurity, ServerSecurityError, load_credentials::load_credentials,
    load_internal_client::load_internal_client, load_server_tls::load_server_tls,
};

/// The TLS, authentication and outbound-credential flags that every Krabka service shares.
///
/// Flatten it into a service CLI with `#[command(flatten)]`, then call
/// [`load`](Self::load) once at start. With no flag set, the service serves
/// plain HTTP with no authentication, as Mimir, Loki, Tempo and Pyroscope do.
#[derive(Debug, Clone, PartialEq, Args)]
pub struct ServerSecurityArgs {
    /// PEM certificate chain that the listeners present. Setting it turns on TLS. Default: unset.
    #[arg(long, env = "KRABKA_SERVER_TLS_CERT_PATH")]
    pub server_tls_cert_path: Option<PathBuf>,

    /// PEM private key for --server-tls-cert-path. Default: unset.
    #[arg(long, env = "KRABKA_SERVER_TLS_KEY_PATH")]
    pub server_tls_key_path: Option<PathBuf>,

    /// PEM CA bundle that client certificates must verify against. Default: unset.
    #[arg(long, env = "KRABKA_SERVER_TLS_CLIENT_CA_PATH")]
    pub server_tls_client_ca_path: Option<PathBuf>,

    /// Whether the listeners ask for a client certificate. Default: `NoClientCert`.
    #[arg(
        long,
        env = "KRABKA_SERVER_TLS_CLIENT_AUTH",
        value_enum,
        default_value = "NoClientCert"
    )]
    pub server_tls_client_auth: ClientAuth,

    /// How long a client may take to finish its TLS handshake, as `10s`. Default: `10s`.
    #[arg(
        long,
        env = "KRABKA_SERVER_TLS_HANDSHAKE_TIMEOUT",
        default_value = "10s",
        value_parser = krabka_units::parse::positive_time
    )]
    pub server_tls_handshake_timeout: Time,

    /// YAML credentials file. Setting it turns on authentication. Default: unset.
    ///
    /// See the `server_security` module documentation for the format.
    #[arg(long, env = "KRABKA_AUTH_CREDENTIALS_CONFIG")]
    pub auth_credentials_config: Option<PathBuf>,

    /// File that holds the bearer token for calls to other Krabka services. Default: unset.
    #[arg(long, env = "KRABKA_INTERNAL_CLIENT_TOKEN_PATH")]
    pub internal_client_token_path: Option<PathBuf>,

    /// PEM client certificate for calls to other Krabka services. Default: unset.
    #[arg(long, env = "KRABKA_INTERNAL_CLIENT_TLS_CERT_PATH")]
    pub internal_client_tls_cert_path: Option<PathBuf>,

    /// PEM private key for --internal-client-tls-cert-path. Default: unset.
    #[arg(long, env = "KRABKA_INTERNAL_CLIENT_TLS_KEY_PATH")]
    pub internal_client_tls_key_path: Option<PathBuf>,

    /// PEM CA bundle that the servers of other Krabka services must verify against. Default: unset.
    #[arg(long, env = "KRABKA_INTERNAL_CLIENT_TLS_CA_PATH")]
    pub internal_client_tls_ca_path: Option<PathBuf>,
}

impl Default for ServerSecurityArgs {
    // The values that `clap` gives when no flag and no variable is set.
    fn default() -> Self {
        Self {
            server_tls_cert_path: None,
            server_tls_key_path: None,
            server_tls_client_ca_path: None,
            server_tls_client_auth: ClientAuth::default(),
            server_tls_handshake_timeout: krabka_units::secs(10),
            auth_credentials_config: None,
            internal_client_token_path: None,
            internal_client_tls_cert_path: None,
            internal_client_tls_key_path: None,
            internal_client_tls_ca_path: None,
        }
    }
}

impl ServerSecurityArgs {
    /// Checks the flags together, reads every file they name, and builds the [`ServerSecurity`].
    ///
    /// # Errors
    ///
    /// Returns a [`ServerSecurityError`] when:
    ///
    /// - a certificate and its key are not set together, for the server or for
    ///   the internal client;
    /// - a client CA is set without a server certificate, or with `NoClientCert`;
    /// - `RequestClientCert` or `RequireAndVerifyClientCert` is set without a
    ///   client CA or without a server certificate;
    /// - a named file cannot be read or parsed. The error names the file.
    pub fn load(&self) -> Result<ServerSecurity, ServerSecurityError> {
        let tls = load_server_tls(self)?;
        let credentials = self
            .auth_credentials_config
            .as_deref()
            .map(load_credentials)
            .transpose()?;
        if let Some(credentials) = &credentials
            && credentials.maps_client_certificates()
            && tls
                .as_ref()
                .is_none_or(|tls| !tls.verifies_client_certificates)
        {
            tracing::warn!(
                "the credentials file maps client certificates, but the listeners do not verify client certificates; no request can authenticate with one"
            );
        }
        let internal_client = load_internal_client(self)?;
        Ok(ServerSecurity::new(tls, credentials, internal_client))
    }
}
