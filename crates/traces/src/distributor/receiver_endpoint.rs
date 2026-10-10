use super::{Arc, CancellationToken, DistributorState, ServerSecurity, SocketAddr};

/// One distributor receiver to start: the address it binds, the state its
/// pushes go through, the security its listener serves with, and the token
/// that stops it.
pub struct ReceiverEndpoint<'a> {
    pub addr: SocketAddr,
    pub state: Arc<DistributorState>,
    pub security: &'a ServerSecurity,
    pub shutdown: CancellationToken,
}
