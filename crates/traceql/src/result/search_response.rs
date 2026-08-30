use super::{ByteSize, TraceResult};

/// Search response.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResponse {
    pub traces: Vec<TraceResult>,
    pub inspected_traces: usize,
    /// Approximate span data the query inspected: the decoded size of the
    /// scanned cold and live batches, before filtering. The engine reports this
    /// value as the Tempo search `metrics.inspectedBytes`.
    pub inspected: ByteSize,
}
