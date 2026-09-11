use super::{
    Args, ClientSecurity, NonEmptyStringValueParser, PathBuf, SaslCredentials, TlsConnectorConfig,
    WalClientSecurityError, WalSaslMechanism, WalSecurityProtocol, check_readable,
    read_password_file,
};

/// The write-ahead log security flags that every Krabka service binary
/// flattens.
///
/// The flags use the Kafka client vocabulary, and each one also reads a
/// `KRABKA_WAL_*` environment variable. With no flag set, the protocol is
/// `PLAINTEXT`. [`Self::load`] checks the flags and builds the client policy.
///
/// The struct holds file paths and a user name, and no secret. Its `Debug`
/// output is safe to log.
#[derive(Args, Clone, Debug, Default, Eq, PartialEq)]
pub struct WalClientSecurityArgs {
    /// Security protocol of the write-ahead log connections, as Kafka's
    /// `security.protocol` names it. Default: `PLAINTEXT`.
    ///
    /// `SSL` and `SASL_SSL` need `--wal-tls-ca-path` and
    /// `--wal-tls-server-name`. `SASL_PLAINTEXT` and `SASL_SSL` need
    /// `--wal-sasl-mechanism`.
    #[arg(
        long,
        env = "KRABKA_WAL_SECURITY_PROTOCOL",
        value_enum,
        ignore_case = true,
        default_value_t = WalSecurityProtocol::Plaintext,
        value_name = "PROTOCOL"
    )]
    pub wal_security_protocol: WalSecurityProtocol,

    /// PEM file of the CA certificates that verify the broker certificate.
    ///
    /// Used by `SSL` and `SASL_SSL`, which need it.
    #[arg(long, env = "KRABKA_WAL_TLS_CA_PATH", value_name = "PATH")]
    pub wal_tls_ca_path: Option<PathBuf>,

    /// Host name that the client sends as TLS SNI and checks the broker
    /// certificate against.
    ///
    /// Used by `SSL` and `SASL_SSL`, which need it. The client uses this one
    /// name for every broker.
    #[arg(
        long,
        env = "KRABKA_WAL_TLS_SERVER_NAME",
        value_name = "NAME",
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub wal_tls_server_name: Option<String>,

    /// PEM certificate chain that the client presents for mutual TLS.
    ///
    /// Used by `SSL` and `SASL_SSL`, where it is optional. It needs
    /// `--wal-tls-key-path`.
    #[arg(long, env = "KRABKA_WAL_TLS_CERT_PATH", value_name = "PATH")]
    pub wal_tls_cert_path: Option<PathBuf>,

    /// PEM private key of the `--wal-tls-cert-path` certificate.
    ///
    /// Used by `SSL` and `SASL_SSL`, where it is optional. It needs
    /// `--wal-tls-cert-path`.
    #[arg(long, env = "KRABKA_WAL_TLS_KEY_PATH", value_name = "PATH")]
    pub wal_tls_key_path: Option<PathBuf>,

    /// SASL mechanism, as Kafka's `sasl.mechanism` names it: `PLAIN`,
    /// `SCRAM-SHA-256`, `SCRAM-SHA-512` or `OAUTHBEARER`.
    ///
    /// Used by `SASL_PLAINTEXT` and `SASL_SSL`, which need it.
    #[arg(
        long,
        env = "KRABKA_WAL_SASL_MECHANISM",
        value_enum,
        value_name = "MECHANISM"
    )]
    pub wal_sasl_mechanism: Option<WalSaslMechanism>,

    /// SASL user name.
    ///
    /// Used by `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`, which need it.
    #[arg(
        long,
        env = "KRABKA_WAL_SASL_USERNAME",
        value_name = "NAME",
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub wal_sasl_username: Option<String>,

    /// File that holds the SASL password. Trailing line breaks are not part
    /// of the password.
    ///
    /// Used by `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`, which need it.
    /// No flag or environment variable takes the password itself, because
    /// those show in `ps` output and in a crash dump.
    #[arg(long, env = "KRABKA_WAL_SASL_PASSWORD_PATH", value_name = "PATH")]
    pub wal_sasl_password_path: Option<PathBuf>,

    /// File that holds the OAUTHBEARER bearer token.
    ///
    /// Used by `OAUTHBEARER`, which needs it. The client reads the file again
    /// for each new connection, so a rotated token needs no restart.
    #[arg(
        long,
        env = "KRABKA_WAL_SASL_OAUTHBEARER_TOKEN_PATH",
        value_name = "PATH"
    )]
    pub wal_sasl_oauthbearer_token_path: Option<PathBuf>,
}

impl WalClientSecurityArgs {
    /// Checks the flags and builds the client security policy that they name.
    ///
    /// `PLAINTEXT` gives `None`, which is the default of
    /// `krabka_client_core::ConnectionOptions` and of the client builders.
    /// Each other protocol gives a policy with the TLS material, the SASL
    /// credentials, or both, that the protocol needs. The function reads the
    /// password file, and checks that each other named file is readable.
    ///
    /// For a TLS protocol, the function also installs the `ring` provider as
    /// the process-wide `rustls` default when no default is installed. The
    /// workspace builds `rustls` with both the `ring` and the `aws-lc-rs`
    /// features, so `rustls` cannot select a provider itself. Without a
    /// default, the first TLS connection panics.
    ///
    /// The returned [`ClientSecurity`] holds the SASL password in memory.
    /// `krabka-client-core` derives `Debug` on `ClientSecurity`, on
    /// `SaslCredentials` and on `ConnectionOptions`, so `{:?}` on any value
    /// that holds this policy prints the password. Never log such a value
    /// with `{:?}`, and never record it in a tracing field.
    ///
    /// # Errors
    ///
    /// Returns [`WalClientSecurityError`] when a flag belongs to a protocol or
    /// a mechanism that is not selected, when the selected protocol or
    /// mechanism does not have a flag that it needs, when the mechanism is
    /// `GSSAPI`, or when the process cannot read a named file.
    pub fn load(&self) -> Result<Option<ClientSecurity>, WalClientSecurityError> {
        let protocol = self.wal_security_protocol;
        let sasl = self.sasl_credentials()?;
        let tls = self.tls_connector_config()?;
        if protocol == WalSecurityProtocol::Plaintext {
            return Ok(None);
        }
        if protocol.requires_tls() {
            crate::server_security::install_crypto_provider();
        }
        Ok(Some(ClientSecurity {
            protocol: protocol.into(),
            tls,
            sasl,
            sasl_host: None,
        }))
    }

    /// The TLS half of the policy, or `None` for a protocol without TLS.
    fn tls_connector_config(&self) -> Result<Option<TlsConnectorConfig>, WalClientSecurityError> {
        let protocol = self.wal_security_protocol;
        if !protocol.requires_tls() {
            let set_flag = [
                ("--wal-tls-ca-path", self.wal_tls_ca_path.is_some()),
                ("--wal-tls-server-name", self.wal_tls_server_name.is_some()),
                ("--wal-tls-cert-path", self.wal_tls_cert_path.is_some()),
                ("--wal-tls-key-path", self.wal_tls_key_path.is_some()),
            ]
            .into_iter()
            .find_map(|(flag, set)| set.then_some(flag));
            return match set_flag {
                Some(flag) => {
                    Err(WalClientSecurityError::TlsFlagWithoutTlsProtocol { flag, protocol })
                }
                None => Ok(None),
            };
        }
        let ca_path = self
            .wal_tls_ca_path
            .clone()
            .ok_or(WalClientSecurityError::MissingTlsCaPath { protocol })?;
        let server_name = self
            .wal_tls_server_name
            .clone()
            .ok_or(WalClientSecurityError::MissingTlsServerName { protocol })?;
        let client_identity = match (&self.wal_tls_cert_path, &self.wal_tls_key_path) {
            (Some(cert_path), Some(key_path)) => Some((cert_path.clone(), key_path.clone())),
            (Some(_), None) => return Err(WalClientSecurityError::TlsCertWithoutKey),
            (None, Some(_)) => return Err(WalClientSecurityError::TlsKeyWithoutCert),
            (None, None) => None,
        };
        check_readable("--wal-tls-ca-path", &ca_path)?;
        if let Some((cert_path, key_path)) = &client_identity {
            check_readable("--wal-tls-cert-path", cert_path)?;
            check_readable("--wal-tls-key-path", key_path)?;
        }
        Ok(Some(TlsConnectorConfig {
            trust_roots_pem: Some(ca_path),
            server_name,
            client_identity,
        }))
    }

    /// The SASL half of the policy, or `None` for a protocol without SASL.
    fn sasl_credentials(&self) -> Result<Option<SaslCredentials>, WalClientSecurityError> {
        let protocol = self.wal_security_protocol;
        if !protocol.requires_sasl() {
            let set_flag = [
                ("--wal-sasl-mechanism", self.wal_sasl_mechanism.is_some()),
                ("--wal-sasl-username", self.wal_sasl_username.is_some()),
                (
                    "--wal-sasl-password-path",
                    self.wal_sasl_password_path.is_some(),
                ),
                (
                    "--wal-sasl-oauthbearer-token-path",
                    self.wal_sasl_oauthbearer_token_path.is_some(),
                ),
            ]
            .into_iter()
            .find_map(|(flag, set)| set.then_some(flag));
            return match set_flag {
                Some(flag) => {
                    Err(WalClientSecurityError::SaslFlagWithoutSaslProtocol { flag, protocol })
                }
                None => Ok(None),
            };
        }
        let mechanism = self
            .wal_sasl_mechanism
            .ok_or(WalClientSecurityError::MissingSaslMechanism { protocol })?;
        let credentials = match mechanism {
            WalSaslMechanism::Gssapi => return Err(WalClientSecurityError::GssapiUnsupported),
            WalSaslMechanism::Plain
            | WalSaslMechanism::ScramSha256
            | WalSaslMechanism::ScramSha512 => {
                if self.wal_sasl_oauthbearer_token_path.is_some() {
                    return Err(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                        flag: "--wal-sasl-oauthbearer-token-path",
                        mechanism,
                    });
                }
                let username = self
                    .wal_sasl_username
                    .clone()
                    .ok_or(WalClientSecurityError::MissingSaslUsername { mechanism })?;
                let password_path = self
                    .wal_sasl_password_path
                    .as_deref()
                    .ok_or(WalClientSecurityError::MissingSaslPasswordPath { mechanism })?;
                let password = read_password_file(password_path)?;
                if mechanism == WalSaslMechanism::Plain {
                    SaslCredentials::Plain { username, password }
                } else {
                    SaslCredentials::Scram {
                        mechanism: mechanism.into(),
                        username,
                        password,
                    }
                }
            }
            WalSaslMechanism::OAuthBearer => {
                if self.wal_sasl_username.is_some() {
                    return Err(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                        flag: "--wal-sasl-username",
                        mechanism,
                    });
                }
                if self.wal_sasl_password_path.is_some() {
                    return Err(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                        flag: "--wal-sasl-password-path",
                        mechanism,
                    });
                }
                let token_path = self
                    .wal_sasl_oauthbearer_token_path
                    .clone()
                    .ok_or(WalClientSecurityError::MissingOAuthBearerTokenPath)?;
                check_readable("--wal-sasl-oauthbearer-token-path", &token_path)?;
                SaslCredentials::OAuthBearer { token_path }
            }
        };
        Ok(Some(credentials))
    }
}
