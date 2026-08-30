use super::*;

/// The compacted HA-tracker topic: `(tenant, cluster) -> elected __replica__`.
pub const HA_TRACKER_TOPIC: &str = "__krabka_metrics_ha";
