use super::EdgeCheckpointEntry;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantEdgeCheckpoints {
    pub tenant: String,
    pub entries: Vec<EdgeCheckpointEntry>,
}
