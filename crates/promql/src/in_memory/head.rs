use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use krabka_blockstore::{LabelMatcher, Labels};
use krabka_metrics::WalRecord;
use krabka_units::prelude::*;

use super::{InMemoryMetricStore, PartitionWatermark, PruneStats};
use crate::{
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{
        ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
        ScanResult, TsdbBlock, TsdbStats,
    },
};

/// Shared hot-head metric store rebuilt from the metrics WAL tail.
///
/// A read takes a snapshot by cloning the inner `Arc` pointer and holds it for
/// the whole scan. A write takes the lock and calls `Arc::make_mut`, so it
/// clones the store whenever a snapshot is outstanding -- which, with a
/// dashboard refreshing, is most of the time.
///
/// Two things keep that clone off the critical path. The store keeps its rows
/// in chunks that a clone shares by pointer, so the copy is bounded by the open
/// chunk rather than by the size of the head; and a WAL-tail poll applies its
/// whole batch through [`WalHead::apply_wal_records_at`], under one lock and
/// one `Arc::make_mut`, rather than one per record.
///
/// A reader never sees a half-applied batch. The writer mutates a store that
/// nothing else can reach -- either because it is unshared, or because
/// `Arc::make_mut` gave it a private copy -- and the mutated store becomes
/// visible only when the write guard drops. A snapshot taken before that point
/// keeps the store it captured, and the chunks inside it are immutable, so it
/// observes the head as of the record before the batch. One taken after
/// observes the head as of the record after it. There is no state in between
/// that a reader can name.
#[derive(Clone, Default)]
pub struct WalHead {
    inner: Arc<RwLock<Arc<InMemoryMetricStore>>>,
}

impl WalHead {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a head with an explicit retention window.
    #[must_use]
    pub fn with_retention(retention: Time) -> Self {
        Self::from_store(InMemoryMetricStore::with_retention(retention))
    }

    #[must_use]
    pub fn from_store(store: InMemoryMetricStore) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(store))),
        }
    }

    /// Applies one decoded metrics WAL record to the shared hot head.
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn apply_wal_record(&self, record: &WalRecord) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).apply_wal_record(record);
    }

    /// Applies one decoded metrics WAL record and advances the offset watermarks
    /// for `partition` to include `offset`.
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn apply_wal_record_at(
        &self,
        record: &WalRecord,
        partition: PartitionIndex,
        offset: Offset,
    ) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        let store = Arc::make_mut(&mut *guard);
        store.apply_wal_record(record);
        store.record_offset(partition, offset);
    }

    /// Applies decoded metrics WAL records in log order.
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn apply_wal_records<'a>(&self, records: impl IntoIterator<Item = &'a WalRecord>) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).apply_wal_records(records);
    }

    /// Applies a batch of decoded metrics WAL records in log order, advancing
    /// each record's partition watermark to its offset.
    ///
    /// This is the WAL tail's entry point, and the reason it exists is that the
    /// per-record form pays one `Arc::make_mut` per record: with a query
    /// holding a snapshot, a poll of a thousand records deep-copied the head a
    /// thousand times. Here the batch takes the write lock once and clones at
    /// most once, whatever its length.
    ///
    /// The batch is published atomically. Nothing observes the store until the
    /// guard drops at the end, so a concurrent query sees either every record
    /// in the batch or none of them, never a prefix.
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn apply_wal_records_at<'a>(
        &self,
        records: impl IntoIterator<Item = (&'a WalRecord, PartitionIndex, Offset)>,
    ) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        let store = Arc::make_mut(&mut *guard);
        for (record, partition, offset) in records {
            store.apply_wal_record(record);
            store.record_offset(partition, offset);
        }
    }

    /// The store a query reads, as of now.
    ///
    /// Cloning the inner `Arc` is the whole cost. The returned store is
    /// immutable and is not affected by any later write, which is what lets a
    /// scan run for as long as it needs without blocking the WAL tail and
    /// without risking a torn view of it.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned.
    pub fn snapshot(&self) -> Arc<InMemoryMetricStore> {
        Arc::clone(&self.inner.read().expect("wal head lock poisoned"))
    }

    /// Drops samples older than the retention window from the shared hot head.
    ///
    /// This method returns how many samples and series it evicted. It does not
    /// change the offset watermarks. The returned stats are advisory, for
    /// metrics and tests. A caller can prune only to bound memory and discard
    /// the stats.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn prune(&self, now_ms: i64) -> PruneStats {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).prune(now_ms)
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn low_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .low_water_offset(partition)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn high_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .high_water_offset(partition)
    }

    /// Snapshot of all per-partition WAL offset watermarks.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn watermarks(&self) -> BTreeMap<PartitionIndex, PartitionWatermark> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .watermarks()
            .clone()
    }

    /// The retention window.
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared metric state is poisoned. Panics if validated series
    /// data is missing an index entry that the operation needs.
    pub fn retention(&self) -> Time {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .retention()
    }
}

#[async_trait::async_trait]
impl MetricStore for WalHead {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let store = self.snapshot();
        store.scan(tenant, matchers, start_ms, end_ms).await
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let store = self.snapshot();
        store.label_names(tenant, matchers, start_ms, end_ms).await
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let store = self.snapshot();
        store
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        let store = self.snapshot();
        store.series(tenant, matchers, start_ms, end_ms).await
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let store = self.snapshot();
        store.exemplars(tenant, matchers, start_ms, end_ms).await
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let store = self.snapshot();
        store.metadata(tenant, metric).await
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let store = self.snapshot();
        store.cardinality_label_names(tenant).await
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        let store = self.snapshot();
        store.cardinality_label_values(tenant).await
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        let store = self.snapshot();
        store.cardinality_active_series(tenant).await
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let store = self.snapshot();
        store.tsdb_stats(tenant).await
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let store = self.snapshot();
        store.tsdb_blocks(tenant).await
    }
}
