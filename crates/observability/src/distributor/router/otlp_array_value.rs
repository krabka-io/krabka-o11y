use super::*;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OtlpArrayValue {
    pub(crate) values: Option<Vec<OtlpAnyValue>>,
}
