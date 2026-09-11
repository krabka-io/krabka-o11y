use super::{
    BTreeMap, DEFAULT_RETENTION, ExemplarRow, FloatRow, HashMap, HistRow, LabelMatcher, Labels,
    MetadataRecord, PartitionIndex, PartitionWatermark, Result, RowChunks, SeriesFingerprint, Time,
    TsdbBlock, prepare_matchers, row_matches,
};

/// In-memory metric store keyed by tenant.
///
/// Everything the WAL appends to lives in a [`RowChunks`], so cloning the
/// store -- which is what `Arc::make_mut` does in [`WalHead`](super::WalHead)
/// while a query holds a snapshot -- shares the sealed chunks by pointer and
/// copies only each tenant's open chunk.
#[derive(Clone)]
pub struct InMemoryMetricStore {
    pub(crate) floats: HashMap<String, RowChunks<FloatRow>>,
    pub(crate) hists: HashMap<String, RowChunks<HistRow>>,
    pub(crate) exemplars: HashMap<String, RowChunks<ExemplarRow>>,
    pub(crate) metadata: HashMap<String, RowChunks<MetadataRecord>>,
    /// Not WAL-written: blocks arrive from the compaction manifest, are few per
    /// tenant, and do not grow with ingest, so a plain vector is enough.
    pub(crate) blocks: HashMap<String, Vec<TsdbBlock>>,
    /// Samples whose timestamp is older than `now_ms - retention` are eligible
    /// for [`InMemoryMetricStore::prune`].
    pub(crate) retention: Time,
    /// WAL offset range currently materialized in the head, keyed by partition.
    /// Offsets track ingestion progress for observability and rebuild bounds.
    /// They are independent of timestamp-based retention.
    pub(crate) watermarks: BTreeMap<PartitionIndex, PartitionWatermark>,
}

impl Default for InMemoryMetricStore {
    fn default() -> Self {
        Self {
            floats: HashMap::new(),
            hists: HashMap::new(),
            exemplars: HashMap::new(),
            metadata: HashMap::new(),
            blocks: HashMap::new(),
            retention: DEFAULT_RETENTION,
            watermarks: BTreeMap::new(),
        }
    }
}

impl InMemoryMetricStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a store with an explicit retention window.
    #[must_use]
    pub fn with_retention(retention: Time) -> Self {
        Self {
            retention,
            ..Self::default()
        }
    }

    /// Returns the retention window.
    #[must_use]
    pub fn retention(&self) -> Time {
        self.retention
    }

    /// Sets the retention window.
    pub fn set_retention(&mut self, retention: Time) {
        self.retention = retention;
    }

    /// Returns the distinct label sets that match the matchers in the time window.
    pub(crate) fn matched_series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        let matchers = prepare_matchers(matchers)?;
        let mut by_fp: BTreeMap<SeriesFingerprint, Labels> = BTreeMap::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows.iter() {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    by_fp
                        .entry(row.fp)
                        .or_insert_with(|| row.labels.as_ref().clone());
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows.iter() {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    by_fp
                        .entry(row.fp)
                        .or_insert_with(|| row.labels.as_ref().clone());
                }
            }
        }
        Ok(by_fp.into_values().collect())
    }
}
