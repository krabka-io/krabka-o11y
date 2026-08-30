use super::*;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanOptions {
    pub job: Option<ScanJob>,
    pub projection_matchers: Vec<SpanMatcher>,
}
