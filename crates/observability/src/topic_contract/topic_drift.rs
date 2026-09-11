use thiserror::Error;

/// One way a live topic differs from the contract.
///
/// The split between fatal and advisory is the whole point of the type. A
/// wrong partition count re-maps keys and destroys order with no error
/// anywhere, so a role that finds one refuses to start. A retention or
/// replication difference is visible in broker metrics and recoverable
/// in place, so a role reports it and runs.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TopicDrift {
    /// The topic does not exist. Fatal.
    #[error("topic {topic} does not exist ({purpose})")]
    Missing {
        topic: String,
        purpose: &'static str,
    },

    /// The broker refused to describe the topic. Fatal, because an
    /// unreadable topic is an unchecked topic.
    #[error("topic {topic} is not readable: {error}")]
    Unreadable { topic: String, error: String },

    /// The partition count differs, so the key-to-shard mapping differs.
    /// Fatal.
    #[error(
        "topic {topic} has {actual} partitions, the contract requires {expected}; \
         changing the count re-maps every key and silently breaks {purpose}"
    )]
    PartitionCount {
        topic: String,
        expected: i32,
        actual: i32,
        purpose: &'static str,
    },

    /// A compacted state topic is not compacted, or the override that says so
    /// is absent. Fatal.
    ///
    /// `actual` is `None` when the topic carries no explicit
    /// `cleanup.policy`. The broker reports per-topic overrides only, so an
    /// absent override is indistinguishable from the broker default -- and
    /// that default is `delete`, which discards the state map at the
    /// retention window.
    #[error(
        "topic {topic} has cleanup.policy={actual}, the contract requires compact; \
         under delete the broker discards {purpose} at the retention window",
        actual = .actual.as_deref().unwrap_or("<unset, so the broker default>")
    )]
    CleanupPolicy {
        topic: String,
        actual: Option<String>,
        purpose: &'static str,
    },

    /// The replication factor differs. Advisory.
    #[error("topic {topic} has replication factor {actual}, the contract asks for {expected}")]
    ReplicationFactor {
        topic: String,
        expected: i32,
        actual: i32,
    },

    /// `retention.ms` differs on a WAL topic. Advisory.
    #[error(
        "topic {topic} has retention.ms={actual}, the contract asks for {expected}; \
         this window bounds how far the block-builder may fall behind before records are dropped",
        actual = .actual.as_deref().unwrap_or("<unset, so the broker default>")
    )]
    Retention {
        topic: String,
        expected: i64,
        actual: Option<String>,
    },
}

impl TopicDrift {
    /// Whether this difference must stop a role from starting.
    ///
    /// Fatal drift is the kind that corrupts data with no further signal: a
    /// re-mapped key space, a state map the broker deletes, or a topic nobody
    /// can read. Advisory drift is durability and housekeeping that an
    /// operator can see and change while the stack runs.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        match self {
            Self::Missing { .. }
            | Self::Unreadable { .. }
            | Self::PartitionCount { .. }
            | Self::CleanupPolicy { .. } => true,
            Self::ReplicationFactor { .. } | Self::Retention { .. } => false,
        }
    }

    /// The topic this difference is about.
    #[must_use]
    pub fn topic(&self) -> &str {
        match self {
            Self::Missing { topic, .. }
            | Self::Unreadable { topic, .. }
            | Self::PartitionCount { topic, .. }
            | Self::CleanupPolicy { topic, .. }
            | Self::ReplicationFactor { topic, .. }
            | Self::Retention { topic, .. } => topic,
        }
    }
}
