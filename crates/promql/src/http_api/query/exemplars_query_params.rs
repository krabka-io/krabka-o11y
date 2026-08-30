use super::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct ExemplarsQueryParams {
    pub(crate) query: String,
    pub(crate) start: String,
    pub(crate) end: String,
}
