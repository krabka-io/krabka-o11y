//! The six topic names the stack uses.
//!
//! They are declared together because they are one table: the provisioning
//! contract, the four signal crates and the metrics ruler all name the same
//! six strings, and a name that drifts in one place creates a seventh topic
//! that nothing reads. Each signal crate re-exports the name it uses.

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
