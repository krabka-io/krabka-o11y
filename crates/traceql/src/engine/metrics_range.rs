use super::{DurationNanos, UnixNano};

#[derive(Clone, Copy)]
pub(crate) struct MetricsRange {
    pub(crate) scan_start: UnixNano,
    pub(crate) scan_end: UnixNano,
    pub(crate) output_start: UnixNano,
    pub(crate) step: DurationNanos,
}
