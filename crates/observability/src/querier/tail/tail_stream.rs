use super::{
    ActiveLogDeleteFilter, Arc, CompactionFrontierSource, LogHotTail, LokiStreamEncoding,
    StreamPlan,
};

pub(crate) struct TailStream {
    pub(crate) plan: StreamPlan,
    pub(crate) source: Option<Arc<dyn LogHotTail>>,
    pub(crate) frontier: CompactionFrontierSource,
    pub(crate) delete_filters: Vec<ActiveLogDeleteFilter>,
    pub(crate) limit: Option<usize>,
    pub(crate) delay_for: i64,
    pub(crate) encoding: LokiStreamEncoding,
    pub(crate) encoding_flags: Vec<String>,
}
