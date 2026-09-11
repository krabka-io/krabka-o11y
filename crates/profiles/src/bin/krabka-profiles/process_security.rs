use super::{Cli, ClientSecurity, ServerSecurity};

/// The security that one process loads from its flags: the posture of its data listeners, and the policy of its WAL connections.
///
/// It has no `Debug`, because the WAL policy holds the SASL password and
/// `krabka-client-core` prints that password under `{:?}`.
#[derive(Clone)]
pub(crate) struct ProcessSecurity {
    /// The TLS and authentication of every data listener.
    pub(crate) server: ServerSecurity,
    /// TLS and SASL for every broker connection. `None` connects in plain text.
    pub(crate) wal: Option<ClientSecurity>,
}

impl ProcessSecurity {
    /// Checks the TLS, credentials and WAL security flags, and reads the files that they name.
    ///
    /// # Errors
    /// Returns an error when a set of flags is not a usable combination, or
    /// when a named file cannot be read or parsed.
    pub(crate) fn load(cli: &Cli) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            server: cli.server_security.load()?,
            wal: cli.wal_security.load()?,
        })
    }
}
