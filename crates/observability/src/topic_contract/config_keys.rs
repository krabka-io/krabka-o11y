//! The Kafka topic-config keys the contract sets and checks.

/// Kafka's per-topic retention key, in milliseconds.
pub const RETENTION_MS: &str = "retention.ms";

/// Kafka's per-topic cleanup-policy key.
pub const CLEANUP_POLICY: &str = "cleanup.policy";

/// The only `cleanup.policy` value that makes a compacted state topic a
/// durable map.
pub const COMPACT: &str = "compact";
