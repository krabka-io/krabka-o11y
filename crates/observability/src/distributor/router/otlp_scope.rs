use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpScope {
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
