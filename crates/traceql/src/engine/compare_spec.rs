use super::UnixNano;

/// The parsed `compare({selection}, topN [, start, end])` spec on a `MetricPlan`.
///
/// Execution scans the outer spanset and splits its spans into two groups. The
/// `selection` group holds the spans that also match `selection`. The
/// `baseline` group holds the rest. Execution then emits per-attribute
/// value-distribution series.
#[derive(Clone)]
pub(crate) struct CompareSpec {
    pub(crate) selection: crate::ast::SpansetExpr,
    pub(crate) top_n: usize,
    pub(crate) start: Option<UnixNano>,
    pub(crate) end: Option<UnixNano>,
}
