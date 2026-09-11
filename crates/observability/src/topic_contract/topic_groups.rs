//! The contract table, and the slice of it each role depends on.
//!
//! A role provisions and validates the topics it reads or writes, not all six.
//! A traces querier that refused to start because a metrics ruler topic was
//! mis-configured would be reporting a fault it does not have.

use super::{
    LOGS_WAL_TOPIC, METRICS_HA_TOPIC, METRICS_RULER_STATE_TOPIC, METRICS_WAL_TOPIC,
    PROFILES_WAL_TOPIC, TRACES_WAL_TOPIC, TopicContract, TopicKind,
};

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
