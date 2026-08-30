use super::RelabelAction;

/// A Prometheus-style relabel rule subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelabelConfig {
    pub source_labels: Vec<String>,
    pub regex: String,
    pub target_label: String,
    pub replacement: String,
    pub action: RelabelAction,
}
