use super::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CompactorDeleteRequest {
    pub(crate) tenant: String,
    pub(crate) request_id: String,
    pub(crate) query: String,
    pub(crate) start_time: i64,
    pub(crate) end_time: i64,
    pub(crate) status: String,
    pub(crate) created_at: i64,
}
