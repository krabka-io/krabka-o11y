use super::{Deserialize, Serialize, TraceBlockStats};

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct TenantTraceIndex {
    pub(crate) blocks: Vec<TraceBlockStats>,
}
