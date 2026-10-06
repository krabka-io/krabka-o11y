use super::{ByteSize, LogicalPlan, SessionContext};

pub(crate) struct PlannedSpanset {
    pub ctx: SessionContext,
    pub plan: LogicalPlan,
    /// Span data that the primary span scan inspected. The engine passes this
    /// value to `SearchResponse::inspected`. Nested structural-join tables scan
    /// the same blocks again, so this field counts only the primary scan.
    pub inspected: ByteSize,
    pub sampling_factor: f64,
    /// A nonempty selected spanset entered the pre-metric scalar pipeline.
    pub spanset_pipeline_had_input: bool,
}
