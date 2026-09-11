//! TLS and SASL for the Kafka connections that carry each write-ahead log.
//!
//! Every signal writes its write-ahead log to a Kafka topic, and the roles
//! read it back from there. A broker can require TLS, mutual TLS or SASL on a
//! listener. This module gives each service binary one set of flags for those
//! connections. [`WalClientSecurityArgs::load`] checks the flags and turns
//! them into the [`ClientSecurity`] policy that `krabka-client-core`
//! negotiates.
//!
//! The policy is opt-in, as it is in the Kafka clients. With no flag set, the
//! protocol is `PLAINTEXT` and `load` returns `None`. `None` is also the value
//! that `ConnectionOptions::default()` and the producer, consumer and admin
//! builders use, so an unconfigured service connects as it did before.
//!
//! # Key Types
//!
//! - [`WalClientSecurityArgs`] — the `clap` flags that a service binary
//!   flattens.
//! - [`WalSecurityProtocol`] and [`WalSaslMechanism`] — the Kafka
//!   `security.protocol` and `sasl.mechanism` values.
//! - [`WalClientSecurityError`] — why a set of flags gives no usable policy.
//! - [`with_client_security`] — puts a policy into a [`ConnectionOptions`].
//!
//! # Applying the Policy
//!
//! Each connection site changes in one line. `security` is the
//! `Option<ClientSecurity>` that `load` returned:
//!
//! - `Producer::builder()` and `Consumer::builder()` — add
//!   `.maybe_security(security.clone())`.
//! - `AdminClient::connect(&bootstrap)` — call
//!   `AdminClient::connect_secured(&bootstrap, security.clone())`.
//! - `AdminClient::connect_with_options(&bootstrap, options)` — pass
//!   `with_client_security(options, security.as_ref())`.
//!
//! # Limits
//!
//! - `krabka-client-core` holds one TLS server name for each policy. The
//!   client sends it as SNI to every broker, and checks every broker
//!   certificate against it. So each broker certificate must be valid for
//!   that one name.
//! - `load` checks that each named file is readable. It does not parse the PEM
//!   files. The first TLS handshake parses them.
//! - GSSAPI is not supported. See [`WalSaslMechanism::Gssapi`].
//!
//! # Secrets
//!
//! The SASL password comes only from a file, never from a flag or an
//! environment variable. A flag shows in `ps` output, and the environment of a
//! process shows in a crash dump. No type in this module holds the password,
//! and no error holds the contents of a file. The [`ClientSecurity`] that
//! `load` returns does hold the password, and `krabka-client-core` prints it
//! under `{:?}`. See [`WalClientSecurityArgs::load`].

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

use clap::{Args, ValueEnum, builder::NonEmptyStringValueParser};
use krabka_client_core::{ClientSecurity, ConnectionOptions, SaslCredentials, TlsConnectorConfig};
use krabka_security::{ListenerProtocol, SaslMechanism};
use thiserror::Error;

mod check_readable;
mod read_password_file;
#[cfg(test)]
mod tests;
mod wal_client_security_args;
mod wal_client_security_error;
mod wal_sasl_mechanism;
mod wal_security_protocol;
mod with_client_security;

use self::{check_readable::check_readable, read_password_file::read_password_file};
pub use self::{
    wal_client_security_args::WalClientSecurityArgs,
    wal_client_security_error::WalClientSecurityError, wal_sasl_mechanism::WalSaslMechanism,
    wal_security_protocol::WalSecurityProtocol, with_client_security::with_client_security,
};
