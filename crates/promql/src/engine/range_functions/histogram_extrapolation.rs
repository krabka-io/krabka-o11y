use super::{RangeFn, Time};

pub(crate) struct HistogramExtrapolation<'a> {
    pub(crate) timestamps: &'a [i64],
    pub(crate) reset_indices: &'a [usize],
    pub(crate) range_start_ms: i64,
    pub(crate) range_end_ms: i64,
    pub(crate) range: Time,
    pub(crate) kind: RangeFn,
}
