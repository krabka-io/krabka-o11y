use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_metrics::{NativeHistogram, SamplePayload, WalRecord};
use krabka_units::prelude::*;

use super::{ExemplarRow, FloatRow, HistRow, InMemoryMetricStore, PartitionWatermark, PruneStats};
use crate::{
    ids::{Offset, PartitionIndex},
    store::{MetadataRecord, TsdbBlock},
};

impl InMemoryMetricStore {
    /// Appends a float sample.
    ///
    /// `labels` is taken as `impl Into<Arc<Labels>>` so a caller that already
    /// holds the series' shared label set -- as
    /// [`InMemoryMetricStore::apply_wal_record`] does -- hands it over instead
    /// of building a second copy per sample.
    pub fn push_float(
        &mut self,
        tenant: &str,
        labels: impl Into<Arc<Labels>>,
        ts_ms: i64,
        value: f64,
    ) {
        self.push_float_with_start_timestamp(tenant, labels, ts_ms, value, None);
    }

    /// Appends a float sample and its optional counter start timestamp.
    pub fn push_float_with_start_timestamp(
        &mut self,
        tenant: &str,
        labels: impl Into<Arc<Labels>>,
        ts_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    ) {
        let labels = labels.into();
        let fp = labels.fingerprint();
        self.floats
            .entry(tenant.to_string())
            .or_default()
            .push(FloatRow {
                fp,
                labels,
                ts_ms,
                value,
                start_timestamp_ms,
            });
    }

    /// Appends a native-histogram sample. See [`InMemoryMetricStore::push_float`]
    /// for why the label set and the histogram arrive as `Into<Arc<_>>`.
    pub fn push_histogram(
        &mut self,
        tenant: &str,
        labels: impl Into<Arc<Labels>>,
        ts_ms: i64,
        hist: impl Into<Arc<NativeHistogram>>,
    ) {
        let labels = labels.into();
        let fp = labels.fingerprint();
        self.hists
            .entry(tenant.to_string())
            .or_default()
            .push(HistRow {
                fp,
                labels,
                ts_ms,
                hist: hist.into(),
            });
    }

    /// Appends an exemplar. See [`InMemoryMetricStore::push_float`] for why the
    /// label sets arrive as `Into<Arc<Labels>>`.
    pub fn push_exemplar(
        &mut self,
        tenant: &str,
        series_labels: impl Into<Arc<Labels>>,
        labels: impl Into<Arc<Labels>>,
        ts_ms: i64,
        value: f64,
    ) {
        self.exemplars
            .entry(tenant.to_string())
            .or_default()
            .push(ExemplarRow {
                series_labels: series_labels.into(),
                labels: labels.into(),
                ts_ms,
                value,
            });
    }

    pub fn push_metadata(
        &mut self,
        tenant: &str,
        metric_family_name: &str,
        metric_type: &str,
        help: &str,
        unit: &str,
    ) {
        self.metadata
            .entry(tenant.to_string())
            .or_default()
            .push(MetadataRecord {
                metric_family_name: metric_family_name.to_string(),
                metric_type: metric_type.to_string(),
                help: help.to_string(),
                unit: unit.to_string(),
            });
    }

    pub fn push_tsdb_block(
        &mut self,
        tenant: &str,
        id: &str,
        min_time: i64,
        max_time: i64,
        num_samples: usize,
        num_series: usize,
    ) {
        self.blocks
            .entry(tenant.to_string())
            .or_default()
            .push(TsdbBlock {
                id: id.to_string(),
                min_time,
                max_time,
                num_samples,
                num_series,
            });
    }

    /// Applies one decoded metrics WAL record to this in-memory head.
    pub fn apply_wal_record(&mut self, record: &WalRecord) {
        // One shared label set for the sample and every exemplar the record
        // carries, so a record costs one label-set allocation rather than one
        // per row, and every row that shares it clones by refcount afterwards.
        let series_labels = Arc::new(record.labels());
        match &record.payload {
            SamplePayload::Float {
                timestamp_ms,
                value,
                start_timestamp_ms,
            } => self.push_float_with_start_timestamp(
                &record.tenant,
                Arc::clone(&series_labels),
                *timestamp_ms,
                *value,
                *start_timestamp_ms,
            ),
            SamplePayload::Hist { timestamp_ms, hist } => {
                self.push_histogram(
                    &record.tenant,
                    Arc::clone(&series_labels),
                    *timestamp_ms,
                    hist.clone(),
                );
            }
            SamplePayload::Metadata {
                metric_family_name,
                metric_type,
                help,
                unit,
            } => self.push_metadata(&record.tenant, metric_family_name, metric_type, help, unit),
            // Neither payload carries a float sample for this head. An
            // exemplar record is drained by the exemplar loop below, and a
            // clock reading publishes its projected series as their own
            // `Float` records, which reach this head through the arm above.
            SamplePayload::Exemplars | SamplePayload::ClockReading(_) => {}
        }
        for exemplar in &record.exemplars {
            self.push_exemplar(
                &record.tenant,
                Arc::clone(&series_labels),
                exemplar.labels.iter().cloned().collect::<Labels>(),
                exemplar.timestamp_ms,
                exemplar.value,
            );
        }
    }

    /// Applies decoded metrics WAL records in log order.
    pub fn apply_wal_records<'a>(&mut self, records: impl IntoIterator<Item = &'a WalRecord>) {
        for record in records {
            self.apply_wal_record(record);
        }
    }

    /// Records that `offset` for `partition` is materialized in the head.
    ///
    /// This method advances the high-water offset. At the first sight of
    /// `partition` it also seeds the low-water offset.
    ///
    /// Offsets track ingestion progress for observability and rebuild bounds;
    /// [`InMemoryMetricStore::prune`] never moves them.
    pub fn record_offset(&mut self, partition: PartitionIndex, offset: Offset) {
        self.watermarks
            .entry(partition)
            .and_modify(|watermark| {
                watermark.low_water_offset = watermark.low_water_offset.min(offset);
                watermark.high_water_offset = watermark.high_water_offset.max(offset);
            })
            .or_insert(PartitionWatermark {
                low_water_offset: offset,
                high_water_offset: offset,
            });
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn low_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.watermarks
            .get(&partition)
            .map(|watermark| watermark.low_water_offset)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn high_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.watermarks
            .get(&partition)
            .map(|watermark| watermark.high_water_offset)
    }

    /// All per-partition WAL offset watermarks materialized in the head.
    #[must_use]
    pub fn watermarks(&self) -> &BTreeMap<PartitionIndex, PartitionWatermark> {
        &self.watermarks
    }

    /// Drops every sample older than `now_ms - retention` from each series.
    ///
    /// This method also removes each series that becomes empty from the
    /// queryable index. It returns the number of evicted samples and series. It
    /// does not touch the offset watermarks: they track ingestion progress, not
    /// retention.
    pub fn prune(&mut self, now_ms: i64) -> PruneStats {
        let cutoff = now_ms.saturating_sub(self.retention.millis_i64());
        let mut stats = PruneStats::default();

        // Fingerprints with at least one surviving sample after pruning.
        let mut live: BTreeSet<SeriesFingerprint> = BTreeSet::new();
        // Fingerprints that had a sample before pruning.
        let mut seen: BTreeSet<SeriesFingerprint> = BTreeSet::new();

        for rows in self.floats.values_mut() {
            for row in rows.iter() {
                seen.insert(row.fp);
            }
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
            for row in rows.iter() {
                live.insert(row.fp);
            }
        }
        for rows in self.hists.values_mut() {
            for row in rows.iter() {
                seen.insert(row.fp);
            }
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
            for row in rows.iter() {
                live.insert(row.fp);
            }
        }
        // Exemplars are not part of the series index, but they are samples that
        // must obey retention so the head stays bounded.
        for rows in self.exemplars.values_mut() {
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
        }

        // Drop the now-empty per-tenant vectors so iteration stays cheap and the
        // tenant disappears from the index once it has no live series.
        self.floats.retain(|_, rows| !rows.is_empty());
        self.hists.retain(|_, rows| !rows.is_empty());
        self.exemplars.retain(|_, rows| !rows.is_empty());

        stats.series_dropped = seen.difference(&live).count();
        stats
    }
}
