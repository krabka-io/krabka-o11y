use super::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpLogRecord {
    pub(crate) time_unix_nano: Value,
    #[serde(default)]
    pub(crate) severity_number: Option<Value>,
    #[serde(default)]
    pub(crate) severity_text: Option<String>,
    pub(crate) body: Option<OtlpAnyValue>,
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
