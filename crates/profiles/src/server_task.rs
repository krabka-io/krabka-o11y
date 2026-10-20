use std::net::SocketAddr;

use tokio::task::JoinHandle;

/// Bound socket and task handle for a supervised profiles HTTP server.
pub struct ServerTask {
    pub address: SocketAddr,
    pub task: JoinHandle<()>,
}

impl ServerTask {
    #[must_use]
    pub fn new(address: SocketAddr, task: JoinHandle<()>) -> Self {
        Self { address, task }
    }
}
