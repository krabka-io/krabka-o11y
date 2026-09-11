use krabka_client_admin::AdminError;
use krabka_units::Time;
use thiserror::Error;

use super::TopicDrift;

/// A topic could not be provisioned, or does not meet the contract.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TopicContractError {
    /// The admin client could not reach the broker or the request failed.
    #[error("topic admin: {0}")]
    Admin(#[from] AdminError),

    /// `CreateTopics` reported a per-topic failure that is not "it already
    /// exists".
    #[error(
        "create topic {topic}: {name} (code {code}){detail}",
        detail = .message.as_deref().map(|m| format!(": {m}")).unwrap_or_default()
    )]
    Create {
        topic: String,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },

    /// One or more topics differ from the contract in a way that corrupts
    /// data if the role runs anyway.
    #[error(
        "topic contract violated:\n  {}",
        .drift.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n  ")
    )]
    Contract { drift: Vec<TopicDrift> },

    /// A partition count of zero or less was asked for.
    #[error("partition count must be positive, got {count}")]
    InvalidPartitionCount { count: i32 },

    /// A replication factor of zero or less was asked for.
    #[error("replication factor must be positive, got {factor}")]
    InvalidReplicationFactor { factor: i32 },

    /// A WAL retention window of zero or less was asked for.
    #[error("WAL retention must be positive, got {retention:?}")]
    InvalidRetention { retention: Time },
}
