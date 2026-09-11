use super::{Deserialize, Metrics, Serialize, TraceJson};

/// The `/api/search` response: the matched traces plus the job-accounting
/// metrics, and whatever the search could not reach.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchResponseJson {
    #[serde(default)]
    pub traces: Vec<TraceJson>,
    #[serde(default)]
    pub metrics: Metrics,
    /// What this answer is missing, one line per cause.
    ///
    /// A fan-out that drops a querier's share and returns 200 has answered a
    /// query with a fraction of the data and said nothing, which is the thing
    /// this milestone exists to stop. The field follows the same shape the
    /// Loki responses use for a skipped block: absent when there is nothing to
    /// say, so an answer that reached everything is byte-identical to what
    /// Tempo returns and Grafana parses unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}
