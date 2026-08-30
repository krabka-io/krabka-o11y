use super::*;

/// A half-edge until both client and server sides arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Edge {
    pub client_service: Option<String>,
    pub server_service: Option<String>,
    pub client_latency_ns: Option<i64>,
    pub server_latency_ns: Option<i64>,
    pub failed: bool,
    pub connection_type: ConnectionType,
    pub first_seen_ns: i64,
}
