#[derive(Debug, Default, PartialEq)]
pub(crate) struct Case {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) query: Option<String>,
    pub(crate) trace_id: Option<u8>,
    pub(crate) expect_trace_ids: Option<String>,
    pub(crate) expect_span_ids: Option<String>,
    pub(crate) expect_series_count: Option<usize>,
    pub(crate) expect_span_count: Option<usize>,
}
