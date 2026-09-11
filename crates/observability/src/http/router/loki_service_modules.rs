/// The modules `/services` reports, in the order single-binary `Loki` lists
/// them.
///
/// The order is part of the page a `Loki` runbook reads, so it is pinned here
/// rather than sorted.
pub(crate) const LOKI_SERVICE_MODULES: &[&str] = &[
    "query-scheduler",
    "ingester-querier",
    "query-frontend",
    "server",
    "querier",
    "rule-evaluator",
    "memberlist-kv",
    "query-frontend-tripperware",
    "analytics",
    "ruler",
    "cache-generation-loader",
    "store",
    "ring",
    "ingester",
    "compactor",
    "distributor",
    "query-scheduler-ring",
];
