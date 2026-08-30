use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelReplace {
    pub query: MetricQuery,
    pub destination_label: String,
    pub replacement: String,
    pub source_label: String,
    pub pattern: String,
}
