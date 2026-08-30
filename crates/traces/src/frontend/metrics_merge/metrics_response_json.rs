use super::{Serialize, Deserialize, MetricSeries};

/// The response body for `/api/metrics/query_range` and
/// `/api/metrics/query`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsResponseJson {
    #[serde(default)]
    pub series: Vec<MetricSeries>,
}
