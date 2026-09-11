use super::TenantId;

/// A by-id job: fetch one trace's spans from one querier.
///
/// By-id does **not** fan per-block, because the querier reassembles a trace
/// across blocks. The frontend fans one job per querier and unions their v2
/// responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceByIdJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub trace_id: [u8; 16],
    pub start_ns: i64,
    pub end_ns: i64,
    /// The `host:port` of the querier this job is assigned to, so that a
    /// fan-out queries each ready querier exactly once.
    ///
    /// It is an address rather than an index into a pool: the pool changes
    /// between refreshes, and an index into a list that has since shrunk names
    /// a different querier than the one it was chosen for.
    pub querier: String,
}
