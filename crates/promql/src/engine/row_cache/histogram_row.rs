use super::*;

#[derive(Clone)]
pub(crate) struct HistogramRow {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) ts_ms: i64,
    pub(crate) hist: NativeHistogram,
}
