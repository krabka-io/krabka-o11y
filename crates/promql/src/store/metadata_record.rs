use super::*;

/// Metric metadata served by `/api/v1/metadata`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRecord {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}
