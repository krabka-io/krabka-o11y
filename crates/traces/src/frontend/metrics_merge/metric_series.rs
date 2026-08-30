use super::*;

/// One metric series: a label set, its Prometheus label string, step-aligned
/// samples, and exemplars.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSeries {
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(rename = "promLabels", default)]
    pub prom_labels: String,
    #[serde(default)]
    pub samples: Vec<MetricSample>,
    #[serde(default)]
    pub exemplars: Vec<Exemplar>,
}
