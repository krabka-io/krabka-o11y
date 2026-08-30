use super::*;

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct TenantTraceIndex {
    pub(crate) blocks: Vec<TraceBlockStats>,
}
