/// Metric metadata entry served by Prometheus-compatible metadata APIs.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricMetadata {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}
