use super::*;

/// The partial result of one by-id job: the typed v2 trace body, which may be
/// empty, plus the accounting.
#[derive(Clone, Debug, Default)]
pub struct TracePartial {
    pub trace: TraceByIdResponseJson,
    pub metrics: Metrics,
}
