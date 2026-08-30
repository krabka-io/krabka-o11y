#[derive(Debug)]
pub(crate) struct DetectedLabelsParams {
    pub(crate) query: Option<String>,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) limit: usize,
}
