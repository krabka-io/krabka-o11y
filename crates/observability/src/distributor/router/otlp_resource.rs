use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpResource {
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
