pub use krabka_observability::wal_consumer_metrics::WalPartitionAssignment;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadRecoveryState {
    pub assignment: Vec<WalPartitionAssignment>,
    pub at_log_end: bool,
}
