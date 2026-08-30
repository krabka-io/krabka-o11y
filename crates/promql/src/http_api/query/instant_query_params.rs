use super::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct InstantQueryParams {
    pub(crate) query: String,
    pub(crate) time: Option<String>,
    pub(crate) limit: Option<usize>,
}
