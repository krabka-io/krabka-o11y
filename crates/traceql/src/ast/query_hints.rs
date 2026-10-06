#[derive(Clone, Debug, Default, PartialEq)]
/// Execution hints carried by a `TraceQL` query.
pub struct QueryHints {
    /// Requests the most recent matching spans first.
    pub most_recent: bool,
    /// Requests exemplar production when set.
    pub exemplars: Option<bool>,
    /// `with(sample=...)`: Tempo's probabilistic metrics-sampling hint.
    ///
    /// Grafana's Traces Drilldown sends `sample=true`. The parser accepts the
    /// boolean adaptive hint and computes exact metrics for it. Numeric fixed
    /// sampling fractions are stored in `values` and use the periodic estimator.
    pub sample: Option<bool>,
    /// Static hints not represented by the boolean execution switches.
    pub values: Vec<(String, super::Value)>,
}

impl QueryHints {
    pub(crate) fn numeric(&self, name: &str) -> Option<f64> {
        // GetFloat in pinned Tempo prefers the first float, then the first int.
        self.values
            .iter()
            .find_map(|(key, value)| match value {
                super::Value::Float(value) if key == name => Some(*value),
                _ => None,
            })
            .or_else(|| {
                self.values.iter().find_map(|(key, value)| match value {
                    super::Value::Int(value) if key == name => {
                        Some(value.to_string().parse().expect("signed integer is finite"))
                    }
                    _ => None,
                })
            })
    }
}
