use super::{ActiveLogDeleteFilter, MetricQuery};

#[derive(Clone, Copy)]
pub(crate) struct MetricWindow<'a> {
    pub(crate) query: &'a MetricQuery,
    pub(crate) eval_times: &'a [i64],
    pub(crate) range_ns: i64,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}
