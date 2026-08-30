use super::{StreamQuery, TimeRange};

#[derive(Clone)]
pub(crate) struct ActiveLogDeleteFilter {
    pub(crate) time_range: TimeRange,
    pub(crate) query: StreamQuery,
}
