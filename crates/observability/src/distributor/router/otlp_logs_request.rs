use super::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpLogsRequest {
    pub(crate) resource_logs: Vec<OtlpResourceLogs>,
}
