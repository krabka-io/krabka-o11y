use super::SeriesFingerprint;

#[derive(Clone, Copy)]
pub(crate) struct FloatRow {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}
