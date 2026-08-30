#[derive(Debug)]
pub(crate) struct DetectedFieldsParams {
    pub(crate) query: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) limit: usize,
    pub(crate) line_limit: usize,
}
