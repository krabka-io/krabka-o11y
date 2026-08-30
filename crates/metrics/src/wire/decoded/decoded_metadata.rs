
/// Metric metadata decoded from an ingest request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedMetadata {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}
