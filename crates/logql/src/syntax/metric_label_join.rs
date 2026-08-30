use super::MetricQuery;

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelJoin {
    pub query: MetricQuery,
    pub destination_label: String,
    pub separator: String,
    pub source_labels: Vec<String>,
}
