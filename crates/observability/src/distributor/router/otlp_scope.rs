use super::{Deserialize, OtlpKeyValue};

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpScope {
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
