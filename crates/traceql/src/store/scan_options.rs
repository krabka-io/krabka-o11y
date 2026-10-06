use super::{ScanJob, SpanMatcher};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanOptions {
    pub job: Option<ScanJob>,
    /// Preserve each attribute's original type for dynamic field expressions.
    pub include_raw_attributes: bool,
    pub projection_matchers: Vec<SpanMatcher>,
}
