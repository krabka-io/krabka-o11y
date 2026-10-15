//! The Kafka topic contract: what the six topics must be, how they are
//! created, and what a role does when a live topic differs.
//!
//! Every signal in this stack writes a Kafka topic and reads it back. The
//! topics carry requirements that nothing on the wire states:
//!
//! - A WAL topic's **partition count is the write-path shard count**. The
//!   producer routes a keyed record with `murmur2(key) % partition_count`, so
//!   the count decides which shard a series, a trace or a stream lives on.
//!   Change it and every key re-maps: a consumer still reads records in
//!   offset order per partition, but one series' history is now split across
//!   two partitions with no order between them. Nothing reports this.
//! - A state topic's **`cleanup.policy` must be `compact`**. The HA tracker
//!   and the ruler keep their state as the last record per key. Under the
//!   Kafka default of `delete` the broker drops the whole map at the
//!   retention window, so an election is re-run and every pending alert
//!   re-fires after a restart.
//! - A WAL topic's **`retention.ms` is backpressure, not housekeeping**. It
//!   is the only limit on how far a stalled block-builder may fall behind
//!   before the broker drops records that were never written to a block.
//!
//! # Create, then check
//!
//! [`ensure_topics`] creates what is missing and then describes every topic
//! and compares it. The two steps are separate because creating proves
//! nothing: `CreateTopics` against an existing topic returns
//! `TOPIC_ALREADY_EXISTS` and leaves the topic exactly as it was, wrong
//! partition count included. [`verify_topics`] is the same check without the
//! create.
//!
//! # One definition of the shard count
//!
//! The partition count is stated once, by the deployment step that provisions
//! the topics, and read back everywhere else. [`verify_topics`] compares it
//! because the caller is that step and knows what it asked for.
//! [`check_topics`] does not: a role checks that its topics exist and that a
//! state topic is compacted, then reads the live shard count out of
//! [`TopicReport::partitions`]. A role that carried its own copy of the number
//! would be the second, independently configured shard count that this
//! contract removes.
//!
//! # What a difference costs
//!
//! [`TopicDrift`] separates the differences that corrupt data from the
//! differences an operator can see and fix while the stack runs.
//! [`TopicDrift::is_fatal`] holds the split, and only fatal drift stops a
//! role from starting.
//!
//! # Who runs it
//!
//! `krabka-o11y-bootstrap` provisions [`ALL_TOPICS`] with
//! [`provision_topics`], as a separate deployment step. Every role calls
//! [`require_topics`] at startup for the topics that role touches -- see
//! [`METRICS_TOPICS`] and its siblings -- and refuses to start when one is
//! absent or not compacted. Concurrent provisioning is expected and safe: one
//! `CreateTopics` wins and the rest read `TOPIC_ALREADY_EXISTS`, which is
//! treated as success and then validated like any other pre-existing topic.
//!
//! # What the broker does not tell us
//!
//! `DescribeConfigs` on this broker returns per-topic **overrides** and
//! nothing else. A key nobody set is absent from the response rather than
//! reported at its effective value, so an absent `cleanup.policy` cannot be
//! told apart from a `cleanup.policy` that happens to equal the broker
//! default. The contract therefore sets both keys explicitly on create, and
//! treats an absent `cleanup.policy` on a state topic as a fatal difference:
//! the Kafka default is `delete`, and assuming otherwise is the silent loss
//! this module exists to stop.

use std::collections::BTreeMap;

use derive_more::Display;
use krabka_client_admin::{
    AdminClient, AdminError, ConfigResource, CreateTopicSpec, DescribeConfigsResults, TopicMetadata,
};
use krabka_client_core::ClientSecurity;
use krabka_units::{Time, convert::TimeExt as _, minutes};
use thiserror::Error;

#[cfg(test)]
mod tests;

mod ensure_topics;
mod verify_topics;

pub use ensure_topics::ensure_topics;
pub use verify_topics::{check_topics, verify_topics};

// The Kafka topic-config keys the contract sets and checks.

/// Kafka's per-topic retention key, in milliseconds.
pub const RETENTION_MS: &str = "retention.ms";

/// Kafka's per-topic cleanup-policy key.
pub const CLEANUP_POLICY: &str = "cleanup.policy";

/// The only `cleanup.policy` value that makes a compacted state topic a
/// durable map.
pub const COMPACT: &str = "compact";

/// What the broker reports about one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTopic {
    /// The topic name.
    pub name: String,

    /// The live partition count, which is the live shard count.
    pub partitions: PartitionCount,

    /// Replicas on the topic's first partition.
    pub replication_factor: i32,

    /// The topic's explicit configuration overrides.
    ///
    /// Broker defaults are absent from this map. `DescribeConfigs` on this
    /// broker returns per-topic overrides only, so a key that nobody set is
    /// reported by its absence and not by its effective value.
    pub overrides: BTreeMap<String, String>,
}

/// The number of partitions on a topic.
///
/// A partition count is not a capacity knob here. The producer routes a keyed
/// record with `murmur2(key) % partition_count`, so the count **is** the
/// write-path shard count: it decides which shard a series, a trace or an
/// entity belongs to. Two readers that disagree about it disagree about where
/// a key lives, and the order a consumer sees for one key stops being the
/// order the producer wrote.
///
/// There is no second place that configures a shard count. Provisioning sets
/// this number once, and every other component derives it from broker
/// metadata -- the producer through its own metadata refresh, and a caller
/// that needs the number through
/// [`shard_count`].
#[derive(Debug, Clone, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionCount(i32);

impl PartitionCount {
    /// Builds a partition count.
    ///
    /// # Errors
    /// Returns [`TopicContractError::InvalidPartitionCount`] when `count` is
    /// not positive. Zero shards accept no records, and Kafka reserves
    /// negative values for "use the broker default", which is the silent
    /// mis-provisioning this contract exists to stop.
    pub const fn new(count: i32) -> Result<Self, TopicContractError> {
        if count <= 0 {
            return Err(TopicContractError::InvalidPartitionCount { count });
        }
        Ok(Self(count))
    }

    /// The count as the `i32` the Kafka protocol carries.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// One topic in the contract: its name, what it is for, and which component
/// depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicContract {
    /// The topic name. Nothing derives it; the broker sees this string.
    pub name: &'static str,

    /// Whether the topic is a WAL or compacted state. This decides the
    /// configuration the contract requires.
    pub kind: TopicKind,

    /// What breaks when the topic is wrong. The provisioning errors quote it,
    /// so an operator reads the consequence rather than only the key.
    pub purpose: &'static str,
}

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
        detail = .message.as_deref().map_or_else(Default::default, |m| format!(": {m}"))
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

/// How much of the contract one check compares against.
///
/// The split exists because the partition count has exactly one definition.
/// The deployment step states it; a role reads it back. A role that carried
/// its own copy to compare would be the second, independently configured
/// shard count that this contract is here to remove.
pub(crate) enum TopicExpectation<'a> {
    /// The deployment's own numbers: partition counts, replication factor and
    /// WAL retention, as `CreateTopics` specs.
    Deployment(&'a [CreateTopicSpec]),

    /// Structure only: the topic exists, and a state topic is compacted.
    /// Both are true or false regardless of how the deployment is sized.
    Structure,
}

/// What a topic is for, and therefore what its configuration must hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicKind {
    /// A write-ahead log. Records are keyed so that one series, trace or
    /// stream stays on one partition, and the block-builder replays them in
    /// offset order.
    ///
    /// `retention.ms` on a WAL topic is not housekeeping. It is the only
    /// backpressure from a stalled block-builder back toward ingest: once the
    /// window passes, the broker drops records the block-builder has not read
    /// yet, and those samples are gone. Size it above the longest
    /// block-builder outage the deployment tolerates.
    Wal,

    /// A log-compacted state topic. The broker keeps the last record per key
    /// and discards the rest, so the topic is a durable map rather than a
    /// stream.
    ///
    /// This only holds with `cleanup.policy=compact`. Under the Kafka default
    /// of `delete`, the broker drops the whole map at the retention window and
    /// the state -- an HA election, a firing alert -- comes back empty.
    CompactedState,
}

// The six topic names the stack uses.
//
// They are declared together because they are one table: the provisioning
// contract, the four signal crates and the metrics ruler all name the same
// six strings, and a name that drifts in one place creates a seventh topic
// that nothing reads. Each signal crate re-exports the name it uses.

/// The metrics write-ahead log. Keyed by `(tenant, series fingerprint)`.
pub const METRICS_WAL_TOPIC: &str = "__krabka_metrics_wal";

/// The traces write-ahead log. Keyed by trace id.
pub const TRACES_WAL_TOPIC: &str = "__krabka_traces_wal";

/// The profiles write-ahead log. Keyed by `(tenant, series fingerprint)`.
pub const PROFILES_WAL_TOPIC: &str = "__krabka_profiles_wal";

/// The logs write-ahead log. Keyed by `(tenant, stream fingerprint)`.
pub const LOGS_WAL_TOPIC: &str = "__krabka_observability_logs_wal";

/// The compacted HA-tracker topic: `(tenant, cluster)` to the elected replica.
pub const METRICS_HA_TOPIC: &str = "__krabka_metrics_ha";

/// The compacted ruler-state topic: alert and rule-group state per entity.
pub const METRICS_RULER_STATE_TOPIC: &str = "__krabka_metrics_ruler_state";

// The contract table, and the slice of it each role depends on.
//
// A role provisions and validates the topics it reads or writes, not all six.
// A traces querier that refused to start because a metrics ruler topic was
// mis-configured would be reporting a fault it does not have.

const METRICS_WAL: TopicContract = TopicContract {
    name: METRICS_WAL_TOPIC,
    kind: TopicKind::Wal,
    purpose: "per-series sample order between the metrics distributor and the block-builder",
};

const METRICS_HA: TopicContract = TopicContract {
    name: METRICS_HA_TOPIC,
    kind: TopicKind::CompactedState,
    purpose: "the elected Prometheus replica per (tenant, cluster)",
};

const METRICS_RULER_STATE: TopicContract = TopicContract {
    name: METRICS_RULER_STATE_TOPIC,
    kind: TopicKind::CompactedState,
    purpose: "alert state across ruler restarts, which stops alerts re-firing from pending",
};

const TRACES_WAL: TopicContract = TopicContract {
    name: TRACES_WAL_TOPIC,
    kind: TopicKind::Wal,
    purpose: "one trace's spans on one partition for the traces block-builder",
};

const PROFILES_WAL: TopicContract = TopicContract {
    name: PROFILES_WAL_TOPIC,
    kind: TopicKind::Wal,
    purpose: "per-series profile order between the profiles distributor and the block-builder",
};

const LOGS_WAL: TopicContract = TopicContract {
    name: LOGS_WAL_TOPIC,
    kind: TopicKind::Wal,
    purpose: "per-stream log order between the logs distributor and the compactor",
};

/// Every topic the stack names. This is what the bootstrap step provisions.
pub const ALL_TOPICS: [TopicContract; 6] = [
    METRICS_WAL,
    METRICS_HA,
    METRICS_RULER_STATE,
    TRACES_WAL,
    PROFILES_WAL,
    LOGS_WAL,
];

/// The topics a `krabka-metrics` role touches. The distributor writes both;
/// the compactor and the querier read the WAL.
pub const METRICS_TOPICS: [TopicContract; 2] = [METRICS_WAL, METRICS_HA];

/// The topics a `krabka-metrics-service` role touches. The ruler reads and
/// writes its own compacted state on top of the metrics WAL.
pub const METRICS_SERVICE_TOPICS: [TopicContract; 3] =
    [METRICS_WAL, METRICS_HA, METRICS_RULER_STATE];

/// The topics a `krabka-traces` role touches.
pub const TRACES_TOPICS: [TopicContract; 1] = [TRACES_WAL];

/// The topics a `krabka-profiles` role touches.
pub const PROFILES_TOPICS: [TopicContract; 1] = [PROFILES_WAL];

/// The topics a `krabka-observability` role touches.
pub const LOGS_TOPICS: [TopicContract; 1] = [LOGS_WAL];

/// What provisioning found. Returned when no fatal drift remains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicReport {
    /// Every topic the call checked, as the broker reports it.
    pub observed: Vec<ObservedTopic>,

    /// Differences that do not stop a role from starting. Callers log these.
    pub advisory: Vec<TopicDrift>,
}

impl TopicReport {
    /// The live shard count for one topic, or `None` when the call did not
    /// check that topic.
    ///
    /// This is the accessor for "how many shards does this signal have". No
    /// setting anywhere else carries that number.
    #[must_use]
    pub fn partitions(&self, topic: &str) -> Option<PartitionCount> {
        self.observed
            .iter()
            .find(|entry| entry.name == topic)
            .map(|entry| entry.partitions)
    }
}

/// The deployment-specific half of the topic contract.
///
/// The fields are public and the struct is built with a literal rather than a
/// constructor on purpose: `wal_partitions` and `state_partitions` are the
/// same type, and a positional constructor would let a caller transpose them
/// without the compiler noticing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicSettings {
    /// Partitions on each of the four WAL topics, which is the write-path
    /// shard count for that signal. Raising it later re-maps every key, so it
    /// is set once at provisioning and checked on every start.
    pub wal_partitions: PartitionCount,

    /// Partitions on the two compacted state topics. Compaction keeps the
    /// last record per key **per partition**, so this count shards the state
    /// map the same way and carries the same one-way hazard.
    pub state_partitions: PartitionCount,

    /// Replicas per partition. One is right for a single-broker development
    /// cluster and wrong for anything that must survive a broker loss.
    pub replication_factor: i32,

    /// `retention.ms` on the WAL topics. It bounds how far the block-builder
    /// may fall behind before the broker starts dropping records it has not
    /// read.
    pub wal_retention: Time,
}

impl TopicSettings {
    /// The single-broker development default: one shard per signal, one
    /// replica, and a fifteen-minute WAL window.
    ///
    /// # Panics
    /// Never. The literal partition counts are positive.
    #[must_use]
    pub fn single_broker() -> Self {
        let one = PartitionCount::new(1).expect("1 is a positive partition count");
        Self {
            wal_partitions: one,
            state_partitions: one,
            replication_factor: 1,
            wal_retention: minutes(15),
        }
    }

    /// Rejects settings the broker would take but the stack cannot use.
    ///
    /// # Errors
    /// Returns [`TopicContractError::InvalidReplicationFactor`] when the
    /// replication factor is not positive, and
    /// [`TopicContractError::InvalidRetention`] when the WAL retention is not
    /// positive. A zero or negative WAL window means the broker may drop a
    /// record the moment it is written.
    pub fn validate(&self) -> Result<(), TopicContractError> {
        if self.replication_factor <= 0 {
            return Err(TopicContractError::InvalidReplicationFactor {
                factor: self.replication_factor,
            });
        }
        if self.wal_retention <= Time::ZERO {
            return Err(TopicContractError::InvalidRetention {
                retention: self.wal_retention,
            });
        }
        Ok(())
    }
}

impl Default for TopicSettings {
    fn default() -> Self {
        Self::single_broker()
    }
}

struct TopicConfig {
    partitions: i32,
    configs: BTreeMap<String, String>,
}

/// Turns the contract and the deployment settings into `CreateTopics` specs.
///
/// A WAL topic gets the WAL partition count and an explicit `retention.ms`. A
/// compacted state topic gets the state partition count and an explicit
/// `cleanup.policy=compact`, and no `retention.ms`: under compaction that key
/// bounds how long a superseded record survives, which is not what the
/// contract wants to state.
///
/// Both keys are set explicitly even where the value matches a broker
/// default. The broker reports per-topic overrides and nothing else, so a key
/// that is left at its default is a key no later start can check.
#[must_use]
pub fn desired_specs(topics: &[TopicContract], settings: &TopicSettings) -> Vec<CreateTopicSpec> {
    topics
        .iter()
        .map(|topic| {
            let config = match topic.kind {
                TopicKind::Wal => {
                    let mut configs = BTreeMap::new();
                    configs.insert(
                        RETENTION_MS.to_string(),
                        settings.wal_retention.millis_i64().to_string(),
                    );
                    TopicConfig {
                        partitions: settings.wal_partitions.get(),
                        configs,
                    }
                }
                TopicKind::CompactedState => {
                    let mut configs = BTreeMap::new();
                    configs.insert(CLEANUP_POLICY.to_string(), COMPACT.to_string());
                    TopicConfig {
                        partitions: settings.state_partitions.get(),
                        configs,
                    }
                }
            };
            CreateTopicSpec {
                name: topic.name.to_string(),
                partitions: config.partitions,
                replicas: settings.replication_factor,
                configs: config.configs,
                ..Default::default()
            }
        })
        .collect()
}

pub(crate) struct TopicInspection {
    pub(crate) observed: Vec<ObservedTopic>,
    pub(crate) drift: Vec<TopicDrift>,
}

/// Kafka's `UNKNOWN_TOPIC_OR_PARTITION`. A broker answers `Metadata` for a
/// topic it does not hold with this code rather than by leaving the topic out
/// of the response, so it reads as "absent" and not as "unreadable".
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Compares what the broker reports against what the contract requires.
///
/// This is the describe-and-compare half of provisioning, and it is a pure
/// function so that every branch is reachable without a broker. It runs
/// whether or not the caller created anything: a `CreateTopics` on a topic
/// that already exists succeeds as a no-op and leaves the wrong configuration
/// in place, so creating proves nothing on its own.
pub(crate) fn inspect_topics(
    contracts: &[TopicContract],
    expectation: &TopicExpectation<'_>,
    metadata: &TopicMetadata,
    configs: &DescribeConfigsResults,
) -> TopicInspection {
    let mut observed = Vec::with_capacity(contracts.len());
    let mut drift = Vec::new();

    for entry in contracts.iter().enumerate() {
        let index = entry.0;
        let contract = entry.1;
        let spec = match expectation {
            TopicExpectation::Deployment(specs) => specs.get(index),
            TopicExpectation::Structure => None,
        };
        let Some(entry) = metadata.topics.iter().find(|t| t.name == contract.name) else {
            drift.push(TopicDrift::Missing {
                topic: contract.name.to_string(),
                purpose: contract.purpose,
            });
            continue;
        };
        if let Some(error) = &entry.error {
            drift.push(if error.code == UNKNOWN_TOPIC_OR_PARTITION {
                TopicDrift::Missing {
                    topic: contract.name.to_string(),
                    purpose: contract.purpose,
                }
            } else {
                TopicDrift::Unreadable {
                    topic: contract.name.to_string(),
                    error: error.name.to_string(),
                }
            });
            continue;
        }
        // A topic with no partitions is a topic the broker does not hold.
        // `Metadata` reports it that way when the topic was deleted between
        // the create and the describe.
        let Ok(partitions) = PartitionCount::new(entry.partition_count) else {
            drift.push(TopicDrift::Missing {
                topic: contract.name.to_string(),
                purpose: contract.purpose,
            });
            continue;
        };

        let resource = ConfigResource::topic(contract.name);
        let overrides = match configs.get(&resource) {
            Some(Ok(config)) => config.dynamic_overrides(&resource),
            Some(Err(error)) => {
                drift.push(TopicDrift::Unreadable {
                    topic: contract.name.to_string(),
                    error: error.name.to_string(),
                });
                continue;
            }
            None => {
                drift.push(TopicDrift::Unreadable {
                    topic: contract.name.to_string(),
                    error: "DescribeConfigs omitted the topic".to_string(),
                });
                continue;
            }
        };

        // Compaction is structural: a state topic under `delete` loses its
        // map whatever the deployment's size, so it is checked even when no
        // deployment numbers were supplied.
        if contract.kind == TopicKind::CompactedState {
            let policy = overrides.get(CLEANUP_POLICY);
            if policy.map(String::as_str) != Some(COMPACT) {
                drift.push(TopicDrift::CleanupPolicy {
                    topic: contract.name.to_string(),
                    actual: policy.cloned(),
                    purpose: contract.purpose,
                });
            }
        }
        if let Some(spec) = spec {
            if partitions.get() != spec.partitions {
                drift.push(TopicDrift::PartitionCount {
                    topic: contract.name.to_string(),
                    expected: spec.partitions,
                    actual: partitions.get(),
                    purpose: contract.purpose,
                });
            }
            if entry.replication_factor != spec.replicas {
                drift.push(TopicDrift::ReplicationFactor {
                    topic: contract.name.to_string(),
                    expected: spec.replicas,
                    actual: entry.replication_factor,
                });
            }
            if contract.kind == TopicKind::Wal {
                let expected = spec.configs.get(RETENTION_MS);
                let actual = overrides.get(RETENTION_MS);
                if expected != actual {
                    drift.push(TopicDrift::Retention {
                        topic: contract.name.to_string(),
                        expected: expected.and_then(|v| v.parse().ok()).unwrap_or_default(),
                        actual: actual.cloned(),
                    });
                }
            }
        }

        observed.push(ObservedTopic {
            name: contract.name.to_string(),
            partitions,
            replication_factor: entry.replication_factor,
            overrides,
        });
    }

    TopicInspection { observed, drift }
}

/// Connects to the broker under `security`, provisions the named topics, and
/// reports what it found.
///
/// This is the call a deployment step makes before the roles start. It logs
/// the live shard count for each topic and warns about every advisory
/// difference, so an under-replicated or short-retention topic is visible
/// without stopping the role. `None` connects in plain text.
///
/// # Errors
/// Returns the error [`ensure_topics`] returns, and a connection error when
/// no bootstrap address answers.
pub async fn provision_topics(
    bootstrap: &str,
    topics: &[TopicContract],
    settings: &TopicSettings,
    security: Option<ClientSecurity>,
) -> Result<TopicReport, TopicContractError> {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_string()], security).await?;
    let report = ensure_topics(&mut admin, topics, settings).await?;
    for topic in &report.observed {
        tracing::info!(
            topic = %topic.name,
            shards = %topic.partitions,
            replication_factor = topic.replication_factor,
            "topic ready",
        );
    }
    for drift in &report.advisory {
        tracing::warn!(topic = %drift.topic(), "{drift}");
    }
    Ok(report)
}

/// Connects to the broker under `security` and refuses to let a role start on
/// topics that are absent or not compacted.
///
/// This is the call a service binary makes at startup, before it builds a
/// producer or joins a consumer group. It creates nothing and states no
/// partition count: the deployment step owns that number, and this reads it
/// back and logs it. `security` is the WAL client security that the binary
/// loaded, and `None` connects in plain text.
///
/// # Errors
/// Returns the error [`check_topics`] returns, and a connection error when no
/// bootstrap address answers.
pub async fn require_topics(
    bootstrap: &str,
    topics: &[TopicContract],
    security: Option<ClientSecurity>,
) -> Result<TopicReport, TopicContractError> {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_string()], security).await?;
    let report = check_topics(&mut admin, topics).await?;
    for topic in &report.observed {
        tracing::info!(
            topic = %topic.name,
            shards = %topic.partitions,
            replication_factor = topic.replication_factor,
            "topic ready",
        );
    }
    Ok(report)
}

/// Reads a topic's live shard count from broker metadata.
///
/// This is the only way to learn the write-path shard count, and it is
/// deliberate that no setting supplies it. The producer routes a keyed record
/// with `murmur2(key) % partition_count` against the partition count it reads
/// from the same metadata, so a number kept anywhere else could only ever
/// disagree with the one that decides where records land.
///
/// # Errors
/// Returns [`TopicContractError::Contract`] when the topic is absent or the
/// broker refuses to report it.
pub async fn shard_count(
    admin: &mut AdminClient,
    topic: &str,
) -> Result<PartitionCount, TopicContractError> {
    let metadata = admin.metadata(&[topic]).await?;
    let missing = || TopicContractError::Contract {
        drift: vec![TopicDrift::Missing {
            topic: topic.to_string(),
            purpose: "the write-path shard count for this signal",
        }],
    };
    let entry = metadata
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(missing)?;
    if let Some(error) = &entry.error {
        return Err(TopicContractError::Contract {
            drift: vec![TopicDrift::Unreadable {
                topic: topic.to_string(),
                error: error.name.to_string(),
            }],
        });
    }
    PartitionCount::new(entry.partition_count).map_err(|_| missing())
}
