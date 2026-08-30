use super::*;

#[derive(Clone)]
pub(crate) struct RankCandidate {
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) labels_key: String,
    pub(crate) sample_index: usize,
    pub(crate) series_index: usize,
    pub(crate) value: f64,
}
