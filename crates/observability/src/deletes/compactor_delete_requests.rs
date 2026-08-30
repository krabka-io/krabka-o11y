use super::{CompactorDeleteRequest, Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CompactorDeleteRequests {
    pub(crate) next_id: u64,
    pub(crate) requests: Vec<CompactorDeleteRequest>,
}
