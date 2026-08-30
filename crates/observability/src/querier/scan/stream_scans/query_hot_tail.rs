use super::{ActiveLogDeleteFilter, CompactionFrontier, WalLogRecord};

#[derive(Clone, Copy)]
pub(crate) struct QueryHotTail<'a> {
    pub(crate) records: &'a [WalLogRecord],
    pub(crate) frontier: &'a CompactionFrontier,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}
