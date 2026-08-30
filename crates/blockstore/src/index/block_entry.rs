use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BlockEntry {
    pub(crate) object_key: String,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
    pub(crate) row_count: usize,
    pub(crate) fingerprints: BTreeSet<SeriesFingerprint>,
}
