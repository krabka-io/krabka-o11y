use std::sync::Arc;

use super::{FloatRow, HistRow, Labels, SeriesFingerprint};

/// The series and timestamp of one float or histogram row.
#[derive(Clone, Copy)]
pub(crate) struct SeriesSampleRef<'a> {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) labels: &'a Arc<Labels>,
    pub(crate) ts_ms: i64,
}

impl FloatRow {
    pub(crate) fn sample_ref(&self) -> SeriesSampleRef<'_> {
        SeriesSampleRef {
            fp: self.fp,
            labels: &self.labels,
            ts_ms: self.ts_ms,
        }
    }
}

impl HistRow {
    pub(crate) fn sample_ref(&self) -> SeriesSampleRef<'_> {
        SeriesSampleRef {
            fp: self.fp,
            labels: &self.labels,
            ts_ms: self.ts_ms,
        }
    }
}
