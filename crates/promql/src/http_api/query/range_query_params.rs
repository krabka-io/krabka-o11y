use super::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct RangeQueryParams {
    pub(crate) query: String,
    pub(crate) start: String,
    pub(crate) end: String,
    pub(crate) step: String,
    pub(crate) limit: Option<usize>,
}
