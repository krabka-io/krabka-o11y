use super::*;

#[derive(Clone)]
pub(crate) struct ActiveLogDeleteFilter {
    pub(crate) time_range: TimeRange,
    pub(crate) query: StreamQuery,
}
