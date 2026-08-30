use super::{TraceJson, Metrics};

/// The partial result of one search job: the matched traces as typed Tempo
/// JSON, plus the job's accounting.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchPartial {
    pub traces: Vec<TraceJson>,
    pub metrics: Metrics,
}
