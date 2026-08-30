#[derive(Debug)]
pub(crate) struct QueryParams {
    pub(crate) query: String,
    pub(crate) time: Option<i64>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
    pub(crate) step: Option<i64>,
    pub(crate) interval: Option<i64>,
    pub(crate) limit: Option<usize>,
    pub(crate) direction: Option<String>,
    pub(crate) delay_for: Option<i64>,
}
