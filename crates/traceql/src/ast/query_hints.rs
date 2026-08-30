#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryHints {
    pub most_recent: bool,
    pub exemplars: Option<bool>,
    /// `with(sample=...)`: Tempo's probabilistic metrics-sampling hint.
    ///
    /// Grafana's Traces Drilldown sends `sample=true`. The parser accepts the
    /// hint and records it here, but Krabka computes exact metrics. Sampling is
    /// a performance hint, so Krabka stays correct when it ignores the hint.
    pub sample: Option<bool>,
}
