use super::{ClientSecurity, ServerSecurity};

/// The loaded security of one `krabka-traces` process.
///
/// [`run`](super::run::run) loads it once from the flags, and every role takes the
/// same value. Each data and ingest listener serves with `server`, and each
/// WAL producer and consumer connects with `wal`.
///
/// The struct has no `Debug`. `wal` holds the SASL password, and
/// `krabka-client-core` prints it under `{:?}`.
#[derive(Clone, Default)]
pub(crate) struct ProcessSecurity {
    /// TLS, authentication, the security-event sink, and the internal client.
    pub(crate) server: ServerSecurity,
    /// The broker connection policy. `None` connects in plain text.
    pub(crate) wal: Option<ClientSecurity>,
}
