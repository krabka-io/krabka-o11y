use super::{ActiveLogDeleteFilter, CompactionFrontier};

pub(crate) struct QueryHotTail<'a, R> {
    pub(crate) records: &'a [R],
    pub(crate) frontier: &'a CompactionFrontier,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}
