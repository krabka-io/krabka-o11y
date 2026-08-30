use super::{Deserialize, OtlpKeyValue};

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OtlpKeyValueList {
    pub(crate) values: Option<Vec<OtlpKeyValue>>,
}
