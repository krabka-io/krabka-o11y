use super::*;

/// The `/api/search` response: the matched traces plus the job-accounting
/// metrics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchResponseJson {
    #[serde(default)]
    pub traces: Vec<TraceJson>,
    #[serde(default)]
    pub metrics: Metrics,
}
