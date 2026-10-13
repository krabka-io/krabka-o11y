use super::{Deserialize, MetricLabelPair, Serialize};

/// An exemplar carried alongside a sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalExemplar {
    pub labels: Vec<MetricLabelPair<String>>,
    pub value: f64,
    pub timestamp_ms: i64,
}
