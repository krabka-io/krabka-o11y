use super::{Deserialize, OtlpLogRecord, OtlpScope};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpScopeLogs {
    pub(crate) scope: Option<OtlpScope>,
    pub(crate) log_records: Vec<OtlpLogRecord>,
}
