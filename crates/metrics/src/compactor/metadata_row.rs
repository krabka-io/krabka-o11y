
/// One metric metadata row ready for indexing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRow {
    pub fingerprint: u64,
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}
