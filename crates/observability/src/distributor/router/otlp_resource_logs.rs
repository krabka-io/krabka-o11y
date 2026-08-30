use super::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpResourceLogs {
    pub(crate) resource: Option<OtlpResource>,
    pub(crate) scope_logs: Vec<OtlpScopeLogs>,
}
