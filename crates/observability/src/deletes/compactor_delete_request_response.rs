use super::*;

#[derive(Serialize)]
pub(crate) struct CompactorDeleteRequestResponse {
    pub(crate) request_id: String,
    pub(crate) start_time: i64,
    pub(crate) end_time: i64,
    pub(crate) query: String,
    pub(crate) status: String,
    pub(crate) created_at: i64,
}
