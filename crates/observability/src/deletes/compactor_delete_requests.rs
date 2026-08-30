use super::*;

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CompactorDeleteRequests {
    pub(crate) next_id: u64,
    pub(crate) requests: Vec<CompactorDeleteRequest>,
}
