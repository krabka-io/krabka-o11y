use super::*;

/// The job-accounting `metrics{}` block. It is additive over completed jobs.
///
/// The Slice 5 querier populates only `total_blocks`, `inspected_traces` and
/// `inspected_bytes` today. The frontend owns `total_jobs`, `completed_jobs`
/// and `inspected_spans`. It seeds them from the plan and sums them across
/// jobs. All six fields serialize, so the merged body carries the full
/// accounting block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub total_jobs: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub completed_jobs: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub total_blocks: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_traces: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_bytes: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_spans: u64,
}

impl Metrics {
    /// Fold another job's accounting into this one, as a field-wise saturating
    /// sum.
    pub fn add(&mut self, other: &Metrics) {
        self.total_jobs = self.total_jobs.saturating_add(other.total_jobs);
        self.completed_jobs = self.completed_jobs.saturating_add(other.completed_jobs);
        self.total_blocks = self.total_blocks.saturating_add(other.total_blocks);
        self.inspected_traces = self.inspected_traces.saturating_add(other.inspected_traces);
        self.inspected_bytes = self.inspected_bytes.saturating_add(other.inspected_bytes);
        self.inspected_spans = self.inspected_spans.saturating_add(other.inspected_spans);
    }
}
