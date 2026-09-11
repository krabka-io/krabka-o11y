use super::{BrokerAccessPolicy, ClientResourcePolicy, ClientSecurity};

/// Parameters needed to connect a broker-backed query authorizer in the
/// background.
///
/// The querier and the block builder both hold one. The querier checks every
/// read and every ruler call against it, and the block builder checks every
/// delete-request call.
///
/// It has no `Debug`, because `security` can hold a SASL password and
/// `krabka-client-core` prints that password under `{:?}`.
#[derive(Clone)]
pub(crate) struct DeferredQueryAuthorizerConnect {
    pub(crate) bootstrap: String,
    pub(crate) topic: String,
    pub(crate) client_resource_policy: ClientResourcePolicy,
    /// The WAL client security that the service loaded. `None` connects in
    /// plain text.
    pub(crate) security: Option<ClientSecurity>,
    pub(crate) access_policy: BrokerAccessPolicy,
}
