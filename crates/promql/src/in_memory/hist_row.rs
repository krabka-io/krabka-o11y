use super::{Labels, NativeHistogram, SeriesFingerprint};

#[derive(Clone)]
pub(crate) struct HistRow {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) labels: Labels,
    pub(crate) ts_ms: i64,
    pub(crate) hist: NativeHistogram,
}
