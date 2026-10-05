/// One evaluation in a versioned query conformance report.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CaseOutcome {
    /// One-based evaluation index within its source file.
    pub ordinal: usize,
    /// Original expression, or original header when parsing failed.
    pub query: String,
    /// Instant, range, or unparsed.
    pub kind: String,
    /// Whether upstream expects the query to be rejected.
    pub expected_rejection: bool,
    /// Matched, mismatch, `expected_divergence`, `feature_disabled`, or uncovered.
    pub status: String,
    /// Exact mismatch or exclusion reason, when present.
    pub detail: Option<String>,
}
