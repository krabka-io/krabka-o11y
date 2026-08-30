use super::*;

#[derive(Debug)]
pub(crate) struct VolumeParams {
    pub(crate) query: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) step: Option<i64>,
    pub(crate) limit: usize,
    pub(crate) target_labels: Option<Vec<String>>,
    pub(crate) aggregate_by: VolumeAggregateBy,
}
