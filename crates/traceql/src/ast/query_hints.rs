#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Execution hints carried by a TraceQL query.
pub struct QueryHints {
    /// Requests the most recent matching spans first.
    pub most_recent: bool,
    /// Requests exemplar production when set.
    pub exemplars: Option<bool>,
    /// `with(sample=...)`: Tempo's probabilistic metrics-sampling hint.
    ///
    /// Grafana's Traces Drilldown sends `sample=true`. The parser accepts the
    /// hint and records it here, but Krabka computes exact metrics. Sampling is
    /// a performance hint, so Krabka stays correct when it ignores the hint.
    pub sample: Option<bool>,
}
