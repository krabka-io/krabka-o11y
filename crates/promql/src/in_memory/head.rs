use std::{
    collections::BTreeMap,
    sync::{Arc, PoisonError, RwLock},
};

use krabka_blockstore::{LabelMatcher, Labels};
use krabka_metrics::WalRecord;
use krabka_units::prelude::*;

use super::{InMemoryMetricStore, PartitionWatermark, PruneStats};
use crate::{
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{
        ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan, MetricStore,
        ScanResult, TsdbBlock, TsdbStats,
    },
};

/// Shared hot-head metric store rebuilt from the metrics WAL tail.
///
/// A read takes a snapshot by cloning the inner `Arc` pointer and holds it for
/// the whole scan. A write builds a private copy of the store, mutates that,
/// and publishes it into the lock in one move.
///
/// The copy is what makes the head survive a fault. Mutating in place would be
/// cheaper while nothing holds a snapshot, but a panic part-way through the
/// mutation would then leave a half-applied store behind the lock and poison
/// it, and every later query and every later WAL record on this querier would
/// panic on the poison. One bad record would permanently break the role while
/// the role went on listening. Publishing a finished copy instead means an
/// unwind drops the copy and leaves the previous store exactly as it was, so
/// the poison flag carries no information and both paths below clear it.
///
/// The copy is cheap by construction: the store keeps its rows in chunks that
/// a clone shares by pointer, so it is bounded by each tenant's open chunk
/// rather than by the size of the head. A WAL-tail poll also applies its whole
/// batch through [`WalHead::apply_wal_records_at`], under one lock and one
/// copy, rather than one per record.
///
/// A reader never sees a half-applied batch. The writer mutates a store that
/// nothing else can reach, and the mutated store becomes visible only when the
/// write guard drops. A snapshot taken before that point keeps the store it
/// captured, and the chunks inside it are immutable, so it observes the head as
/// of the record before the batch. One taken after observes the head as of the
/// record after it. There is no state in between that a reader can name.
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

    /// Applies `apply` to the shared head and publishes the result.
    ///
    /// The head changes only if `apply` returns. Should it panic, the copy it
    /// was writing goes with the unwind and the next reader sees the head as
    /// the last successful update left it.
    ///
    /// This is the single write path, and every other mutating method here is
    /// one line over it. It is public because it is also the seam that lets a
    /// test provoke a fault part-way through an update.
    pub fn update<R>(&self, apply: impl FnOnce(&mut InMemoryMetricStore) -> R) -> R {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        // Cloning the pointer before `make_mut` is what forces the private
        // copy: with the guard's own reference still outstanding the store is
        // never unshared, so the mutation below cannot reach what a reader or
        // the next writer would see.
        let mut next = Arc::clone(&guard);
        let outcome = apply(Arc::make_mut(&mut next));
        *guard = next;
        outcome
    }

    /// Applies one decoded metrics WAL record to the shared hot head.
    pub fn apply_wal_record(&self, record: &WalRecord) {
        self.update(|store| store.apply_wal_record(record));
    }

    /// Applies one decoded metrics WAL record and advances the offset watermarks
    /// for `partition` to include `offset`.
    pub fn apply_wal_record_at(
        &self,
        record: &WalRecord,
        partition: PartitionIndex,
        offset: Offset,
    ) {
        self.update(|store| {
            store.apply_wal_record(record);
            store.record_offset(partition, offset);
        });
    }

    /// Applies decoded metrics WAL records in log order.
    pub fn apply_wal_records<'a>(&self, records: impl IntoIterator<Item = &'a WalRecord>) {
        self.update(|store| store.apply_wal_records(records));
    }

    /// Applies a batch of decoded metrics WAL records in log order, advancing
    /// each record's partition watermark to its offset.
    ///
    /// This is the WAL tail's entry point, and the reason it exists is that the
    /// per-record form pays one store copy per record: a poll of a thousand
    /// records copied the head a thousand times. Here the batch takes the write
    /// lock once and copies once, whatever its length.
    ///
    /// The batch is published atomically. Nothing observes the store until the
    /// guard drops at the end, so a concurrent query sees either every record
    /// in the batch or none of them, never a prefix. A record that panics
    /// part-way discards the whole batch rather than publishing a prefix.
    pub fn apply_wal_records_at<'a>(
        &self,
        records: impl IntoIterator<Item = (&'a WalRecord, PartitionIndex, Offset)>,
    ) {
        self.update(|store| {
            for (record, partition, offset) in records {
                store.apply_wal_record(record);
                store.record_offset(partition, offset);
            }
        });
    }

    /// The store a query reads, as of now.
    ///
    /// Cloning the inner `Arc` is the whole cost. The returned store is
    /// immutable and is not affected by any later write, which is what lets a
    /// scan run for as long as it needs without blocking the WAL tail and
    /// without risking a torn view of it.
    #[must_use]
    pub fn snapshot(&self) -> Arc<InMemoryMetricStore> {
        Arc::clone(&self.inner.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Drops samples older than the retention window from the shared hot head.
    ///
    /// This method returns how many samples and series it evicted. It does not
    /// change the offset watermarks. The returned stats are advisory, for
    /// metrics and tests. A caller can prune only to bound memory and discard
    /// the stats.
    #[must_use]
    pub fn prune(&self, now_ms: i64) -> PruneStats {
        self.update(|store| store.prune(now_ms))
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn low_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.snapshot().low_water_offset(partition)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn high_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.snapshot().high_water_offset(partition)
    }

    /// Snapshot of all per-partition WAL offset watermarks.
    #[must_use]
    pub fn watermarks(&self) -> BTreeMap<PartitionIndex, PartitionWatermark> {
        self.snapshot().watermarks().clone()
    }

    /// The retention window.
    #[must_use]
    pub fn retention(&self) -> Time {
        self.snapshot().retention()
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
    ) -> Result<ExemplarScan> {
        let store = self.snapshot();
        store.exemplars(tenant, matchers, start_ms, end_ms).await
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<MetadataScan> {
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
