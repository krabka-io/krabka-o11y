use super::case_features::CaseFeatures;

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
    /// Parsed type/composition witnesses; these do not imply passing execution.
    pub features: CaseFeatures,
    /// Actual engine rejection before expectation handling, including `eval_fail`.
    pub evaluation_error: Option<String>,
    /// Source-bound functions/aggregates disabled in this build configuration.
    pub disabled_features: Vec<String>,
    /// Original semantic verdict before configuration classification.
    pub observed_status: String,
    /// Original semantic mismatch remains visible when classified as disabled.
    pub observed_detail: Option<String>,
    /// Matched, mismatch, `expected_divergence`, `feature_disabled`, or uncovered.
    pub status: String,
    /// Exact mismatch or exclusion reason, when present.
    pub detail: Option<String>,
}
