use std::sync::{Arc, Weak};

use super::{
    BTreeMap, DEFAULT_RETENTION, ExemplarRow, FloatHeadSummary, FloatRow, HashMap, HistRow,
    LabelMatcher, Labels, MetadataRecord, PartitionIndex, PartitionWatermark, Result, RowChunks,
    SeriesFingerprint, SeriesSampleRef, Time, TsdbBlock, matcher::PreparedMatcher,
    prepare_matchers, row_matches,
};

type SeriesLabelCache = HashMap<SeriesFingerprint, Vec<Weak<Labels>>>;

/// In-memory metric store keyed by tenant.
///
/// Everything the WAL appends to lives in `RowChunks`, so cloning the
/// store -- which is what `Arc::make_mut` does in [`WalHead`](super::WalHead)
/// while a query holds a snapshot -- shares the sealed chunks by pointer and
/// copies only each tenant's open chunk. A float append also copies the
/// affected tenant's shared series summary once per WAL batch.
#[derive(Clone)]
pub struct InMemoryMetricStore {
    pub(crate) floats: HashMap<String, RowChunks<FloatRow>>,
    /// Shared with snapshots; a WAL batch copies the affected tenant's series
    /// summary on its first float append.
    pub(crate) float_head_summaries: HashMap<String, Arc<FloatHeadSummary>>,
    pub(crate) hists: HashMap<String, RowChunks<HistRow>>,
    pub(crate) exemplars: HashMap<String, RowChunks<ExemplarRow>>,
    pub(crate) metadata: HashMap<String, RowChunks<MetadataRecord>>,
    /// Share a series' immutable labels across WAL records, not just within
    /// one record. Weak entries let rows and snapshots determine their lifetime.
    pub(crate) series_labels: HashMap<String, Arc<SeriesLabelCache>>,
    /// Not WAL-written: blocks arrive from the compaction manifest, are few per
    /// tenant, and do not grow with ingest, so a plain vector is enough.
    pub(crate) blocks: HashMap<String, Vec<TsdbBlock>>,
    /// Samples whose timestamp is older than `now_ms - retention` are eligible
    /// for [`crate::InMemoryMetricStore::prune`].
    pub(crate) retention: Time,
    /// Lower bound on every retained sample timestamp. Deleting a tenant may
    /// leave it conservatively low; pruning recomputes the exact minimum.
    pub(crate) oldest_sample_timestamp_ms: Option<i64>,
    /// WAL offset range currently materialized in the head, keyed by partition.
    /// Offsets track ingestion progress for observability and rebuild bounds.
    /// They are independent of timestamp-based retention.
    pub(crate) watermarks: BTreeMap<PartitionIndex, PartitionWatermark>,
}

impl Default for InMemoryMetricStore {
    fn default() -> Self {
        Self {
            floats: HashMap::new(),
            float_head_summaries: HashMap::new(),
            hists: HashMap::new(),
            exemplars: HashMap::new(),
            metadata: HashMap::new(),
            series_labels: HashMap::new(),
            blocks: HashMap::new(),
            retention: DEFAULT_RETENTION,
            oldest_sample_timestamp_ms: None,
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
        Ok(self
            .matched_series_shared(tenant, matchers, start_ms, end_ms)?
            .into_iter()
            .map(|labels| labels.as_ref().clone())
            .collect())
    }

    pub(crate) fn matched_series_shared(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Arc<Labels>>> {
        let matchers = prepare_matchers(matchers)?;
        let selection = RowSelection {
            matchers: &matchers,
            start_ms,
            end_ms,
        };
        let mut by_fp: BTreeMap<SeriesFingerprint, Arc<Labels>> = BTreeMap::new();
        if let Some(rows) = self.floats.get(tenant) {
            let selected = self.float_head_summaries.get(tenant).and_then(|summary| {
                summary.matching_series(rows.len(), &matchers, start_ms, start_ms, end_ms)
            });
            if let Some((series, _)) = selected {
                by_fp.extend(
                    series
                        .into_iter()
                        .map(|entry| (entry.latest.0, Arc::clone(&entry.labels))),
                );
            } else {
                insert_matching_series(
                    &mut by_fp,
                    rows.iter().map(FloatRow::sample_ref),
                    &selection,
                );
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            insert_matching_series(&mut by_fp, rows.iter().map(HistRow::sample_ref), &selection);
        }
        Ok(by_fp.into_values().collect())
    }
}

/// Prepared matchers and the inclusive millisecond window a row must match.
struct RowSelection<'a> {
    matchers: &'a [PreparedMatcher],
    start_ms: i64,
    end_ms: i64,
}

/// Adds the labels of each of `rows` that `selection` matches, unless
/// `by_fp` already holds its series.
fn insert_matching_series<'a>(
    by_fp: &mut BTreeMap<SeriesFingerprint, Arc<Labels>>,
    rows: impl Iterator<Item = SeriesSampleRef<'a>>,
    selection: &RowSelection<'_>,
) {
    for row in rows {
        if !by_fp.contains_key(&row.fp)
            && row_matches(
                row.fp,
                row.labels,
                row.ts_ms,
                selection.matchers,
                selection.start_ms,
                selection.end_ms,
            )
        {
            by_fp
                .entry(row.fp)
                .or_insert_with(|| Arc::clone(row.labels));
        }
    }
}
