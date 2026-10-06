//! Metrics: the block writer and its compaction manifests, the level
//! compactor, the retention sweep, and `PromQL` over the manifests.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use krabka_blockstore::{
    BlockLevel, BlockStore, BlockTimestampUnit, BlockWriter, CompactionPolicy,
    DEFAULT_BLOCK_READ_MAX, Labels, RetentionWindows, TenantId,
};
use krabka_metrics::{
    DeferredBlockDeletions, FloatRow, IngestEnforcer, Limits, ObjectStoreCompactionIndexSink,
    TenantCompactionRows, compact_metric_blocks_once, enforce_compaction_retention,
    list_compaction_manifests, write_compacted_tenant_blocks,
};
use krabka_promql::{EngineOpts, MetricBlockStore, PromqlEngine, QueryResult, SampleValue};
use krabka_units::{Time, hours, per_sec, secs};
use num_traits::ToPrimitive as _;
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use super::{Batch, Signal, Stores, WriteOutcome, config::mix, due, err, now_ms};

const METRIC: &str = "soak_samples";

pub struct MetricsSignal {
    stores: Stores,
    seed: u64,
    enforcer: IngestEnforcer,
    limits: Mutex<HashMap<String, Limits>>,
    reader: AsyncMutex<Option<(Instant, Arc<PromqlEngine<MetricBlockStore>>)>>,
    deferred: AsyncMutex<DeferredBlockDeletions>,
}

struct Windows(Time);

impl RetentionWindows for Windows {
    fn block_retention(&self, _tenant: &str) -> Time {
        self.0
    }
}

impl MetricsSignal {
    /// Metrics keeps no index of its own: the manifests are the index, so
    /// opening reads nothing until the first query lists them.
    pub fn open(stores: Stores, seed: u64) -> Self {
        Self {
            stores,
            seed,
            enforcer: IngestEnforcer::new(),
            limits: Mutex::new(HashMap::new()),
            reader: AsyncMutex::new(None),
            deferred: AsyncMutex::new(DeferredBlockDeletions::new()),
        }
    }

    fn tenant_limits(&self, tenant: &str) -> Limits {
        self.limits
            .lock()
            .expect("the limits lock is not poisoned")
            .get(tenant)
            .cloned()
            .unwrap_or_else(|| Limits {
                ingestion_rate: per_sec(0),
                ..Limits::default()
            })
    }

    async fn engine(&self, fresh: bool) -> Result<Arc<PromqlEngine<MetricBlockStore>>, String> {
        let mut reader = self.reader.lock().await;
        if let Some((at, engine)) = reader.as_ref()
            && !due(Some(*at), fresh)
        {
            return Ok(Arc::clone(engine));
        }
        let manifests = list_compaction_manifests(&self.stores.read)
            .await
            .map_err(err)?;
        let base = url::Url::parse("memory:///").map_err(err)?;
        let store = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&self.stores.read), base),
            None,
            &manifests,
        );
        let engine = Arc::new(PromqlEngine::new(Arc::new(store), EngineOpts::default()));
        *reader = Some((Instant::now(), Arc::clone(&engine)));
        Ok(engine)
    }
}

#[async_trait]
impl Signal for MetricsSignal {
    async fn write(&self, tenant: &str, batch: Batch) -> Result<WriteOutcome, String> {
        if self
            .enforcer
            .check_sample_rate(&self.tenant_limits(tenant), tenant, batch.rows())
            .is_err()
        {
            return Ok(WriteOutcome::Rejected);
        }
        let now = now_ms();
        let mut series_labels = BTreeMap::new();
        let mut float_rows = Vec::new();
        for series in 0..batch.series {
            let labels = Labels::from_pairs([
                ("__name__", METRIC.to_string()),
                ("series", series.to_string()),
            ]);
            let fingerprint = labels.fingerprint();
            series_labels.insert(fingerprint, labels.into());
            for point in 0..batch.samples {
                let value = mix(self.seed, batch.seq, series, point) % 1_000;
                float_rows.push(FloatRow {
                    fingerprint,
                    timestamp_ms: now - i64::from(batch.samples - 1 - point),
                    value: value.to_f64().unwrap_or(0.0),
                    start_timestamp_ms: None,
                });
            }
        }
        let rows = TenantCompactionRows {
            tenant: tenant.to_string(),
            series_labels,
            float_rows,
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        write_compacted_tenant_blocks(
            &BlockWriter::new(Arc::clone(&self.stores.write)),
            &ObjectStoreCompactionIndexSink::new(Arc::clone(&self.stores.write)),
            &rows,
            batch.seq,
            batch.seq,
        )
        .await
        .map_err(err)?;
        Ok(WriteOutcome::Accepted)
    }

    async fn query(&self, tenant: &str, window: Duration, fresh: bool) -> Result<u64, String> {
        let engine = self.engine(fresh).await?;
        let tenant = TenantId::new(tenant).map_err(err)?;
        let query = format!(
            "count(count_over_time({METRIC}[{}s]))",
            window.as_secs().max(1)
        );
        match engine
            .query_instant(&tenant, &query, now_ms())
            .await
            .map_err(err)?
        {
            QueryResult::InstantVector(samples) => Ok(samples
                .iter()
                .filter_map(|sample| match sample.value {
                    SampleValue::Float(value) => value.to_u64(),
                    SampleValue::Histogram(_) => None,
                })
                .sum()),
            other => Err(format!("{query} returned {other:?}, not an instant vector")),
        }
    }

    async fn compact(&self) -> Result<Value, String> {
        let store = &self.stores.maintenance;
        let mut deferred = self.deferred.lock().await;
        let pass = compact_metric_blocks_once(
            store,
            &BlockWriter::new(Arc::clone(store)),
            &ObjectStoreCompactionIndexSink::new(Arc::clone(store)),
            CompactionPolicy::new(
                8,
                usize::MAX,
                BlockLevel(4),
                hours(24),
                BlockTimestampUnit::Millis,
            ),
            DEFAULT_BLOCK_READ_MAX,
            &mut deferred,
        )
        .await
        .map_err(err)?;
        let failures = pass.manifests_retired.failures.len() + pass.blocks_deleted.failures.len();
        if failures > 0 {
            return Err(format!("{failures} compaction deletions failed"));
        }
        Ok(json!({
            "outputs": pass.outputs.len(),
            "manifests_retired": pass.manifests_retired.deleted,
            "blocks_deleted": pass.blocks_deleted.deleted,
        }))
    }

    async fn expire(&self, retention_secs: u32) -> Result<Value, String> {
        let stats = enforce_compaction_retention(
            &self.stores.maintenance,
            SystemTime::now(),
            &Windows(secs(retention_secs)),
        )
        .await
        .map_err(err)?;
        let failures = stats.failures().count();
        if failures > 0 {
            return Err(format!("{failures} retention deletions failed"));
        }
        Ok(json!({
            "manifests_scanned": stats.manifests_scanned,
            "manifests_retired": stats.manifests_retired.deleted,
            "blocks_deleted": stats.blocks_deleted.deleted,
        }))
    }

    fn limit(&self, tenant: &str, rows_per_sec: u32, burst_rows: u64) {
        self.limits
            .lock()
            .expect("the limits lock is not poisoned")
            .insert(
                tenant.to_string(),
                Limits {
                    ingestion_rate: per_sec(rows_per_sec),
                    ingestion_burst_size: burst_rows,
                    ..Limits::default()
                },
            );
    }

    fn rate_limiter(&self) -> Value {
        json!({
            "status": "applied",
            "detail": "krabka_metrics::IngestEnforcer::check_sample_rate, the distributor's \
                       per-tenant token bucket; unlimited unless a phase sets a limit",
        })
    }
}
