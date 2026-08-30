
/// A by-id job: fetch one trace's spans from one querier.
///
/// By-id does **not** fan per-block, because the querier reassembles a trace
/// across blocks. The frontend fans one job per querier and unions their v2
/// responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceByIdJobRequest {
    pub tenant: String,
    pub trace_id: [u8; 16],
    pub start_ns: i64,
    pub end_ns: i64,
    /// Index into the backend's querier pool to target, so that a fan-out
    /// queries each querier exactly once. `None` lets the backend pick one,
    /// round-robin.
    pub querier: Option<usize>,
}
