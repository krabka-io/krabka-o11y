use super::*;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OtlpKeyValue {
    pub(crate) key: String,
    pub(crate) value: OtlpAnyValue,
}
