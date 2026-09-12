use super::{Deserialize, OtlpAnyValue, OtlpKeyValue, Value};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpLogRecord {
    pub(crate) time_unix_nano: Value,
    #[serde(default)]
    pub(crate) severity_number: Option<Value>,
    #[serde(default)]
    pub(crate) severity_text: Option<String>,
    #[serde(default)]
    pub(crate) trace_id: Option<String>,
    #[serde(default)]
    pub(crate) span_id: Option<String>,
    pub(crate) body: Option<OtlpAnyValue>,
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
