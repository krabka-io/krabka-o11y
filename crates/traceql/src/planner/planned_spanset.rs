use super::{ByteSize, LogicalPlan, SessionContext};

pub(crate) struct PlannedSpanset {
    pub ctx: SessionContext,
    pub plan: LogicalPlan,
    /// Span data that the primary span scan inspected. The engine passes this
    /// value to `SearchResponse::inspected`. Nested structural-join tables scan
    /// the same blocks again, so this field counts only the primary scan.
    pub inspected: ByteSize,
}
