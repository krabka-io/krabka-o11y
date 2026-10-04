//! Traces: the block builder and its index snapshot, the level compactor, the
//! retention pass, and `TraceQL` search through the querier's span store.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use krabka_blockstore::{
    BlockLevel, BlockStore, BlockTimestampUnit, BlockWriter, CompactionPolicy,
    DEFAULT_INDEX_SNAPSHOT_MAX, TraceIndex,
};
use krabka_traceql::{EngineOpts, TraceqlEngine};
use krabka_traces::{
    AttrValue, KeyValue, Limits, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::build_blocks,
    compactor::{compact_once, delete_trace_blocks, expire_trace_blocks},
    ids::UnixNano,
    limits::{IngestEnforcer, OverridesProvider},
    querier::store::{KrabkaSpanStore, SharedTraceIndex},
};
use krabka_units::{days, per_sec};
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use super::{Batch, Signal, Stores, WriteOutcome, config::mix, due, err, now_ns, window_ns};

const INDEX_KEY: &str = "index/traces.json";

/// The most traces one search returns. Above the largest batch the soak
/// writes, so the count is the number of traces in the window up to here.
const SEARCH_LIMIT: usize = 10_000;

pub struct TracesSignal {
    stores: Stores,
    seed: u64,
    enforcer: IngestEnforcer,
    limits: Mutex<HashMap<String, Limits>>,
    /// Each write builds its own index, as one block builder per partition
    /// does, and publishes it by merging into the latest snapshot.
    reader: AsyncMutex<Option<(Instant, Arc<TraceqlEngine<KrabkaSpanStore>>)>>,
    /// Serializes compaction and retention, as the compactor's one loop does.
    compactor: AsyncMutex<()>,
}

impl TracesSignal {
    pub fn open(stores: Stores, seed: u64) -> Self {
        Self {
            stores,
            seed,
            enforcer: IngestEnforcer::new(),
            limits: Mutex::new(HashMap::new()),
            reader: AsyncMutex::new(None),
            compactor: AsyncMutex::new(()),
        }
    }

    fn tenant_limits(&self, tenant: &str) -> Limits {
        self.limits
            .lock()
            .expect("the limits lock is not poisoned")
            .get(tenant)
            .copied()
            .unwrap_or(Limits {
                ingestion_rate: per_sec(0),
                ..Limits::default()
            })
    }

    async fn engine(&self, fresh: bool) -> Result<Arc<TraceqlEngine<KrabkaSpanStore>>, String> {
        let mut reader = self.reader.lock().await;
        if let Some((at, engine)) = reader.as_ref()
            && !due(Some(*at), fresh)
        {
            return Ok(Arc::clone(engine));
        }
        let index = TraceIndex::load_latest_snapshot(&self.stores.read, INDEX_KEY)
            .await
            .map_err(err)?;
        let shared: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(index));
        let base = url::Url::parse("memory:///").map_err(err)?;
        let blocks = Arc::new(BlockStore::new(Arc::clone(&self.stores.read), base));
        let engine = Arc::new(TraceqlEngine::new(
            Arc::new(
                KrabkaSpanStore::new(blocks, shared, None)
                    .with_index_snapshot(INDEX_KEY.into(), DEFAULT_INDEX_SNAPSHOT_MAX),
            ),
            EngineOpts::default(),
        ));
        *reader = Some((Instant::now(), Arc::clone(&engine)));
        Ok(engine)
    }

    /// Runs `pass` over the latest index and saves the index before any
    /// object is deleted.
    async fn with_compactor_index<F>(&self, pass: F) -> Result<Value, String>
    where
        F: AsyncFnOnce(&mut TraceIndex) -> Result<(Vec<String>, Value), String>,
    {
        let store = &self.stores.maintenance;
        let _serial = self.compactor.lock().await;
        // A fresh load every pass: the block builders publish by merging into
        // the snapshot, and the compactor must see what they added.
        let mut index = TraceIndex::load_latest_snapshot(store, INDEX_KEY)
            .await
            .map_err(err)?;
        let (retired, mut report) = pass(&mut index).await?;
        if !retired.is_empty() {
            index
                .save_latest_snapshot(store, INDEX_KEY)
                .await
                .map_err(err)?;
            let deletions = delete_trace_blocks(store, &retired).await;
            if !deletions.failures.is_empty() {
                return Err(format!(
                    "{} block deletions failed",
                    deletions.failures.len()
                ));
            }
            report["blocks_deleted"] = json!(deletions.blocks_deleted);
        }
        Ok(report)
    }
}

#[async_trait]
impl Signal for TracesSignal {
    async fn write(&self, tenant: &str, batch: Batch) -> Result<WriteOutcome, String> {
        if self
            .enforcer
            .check_span_rate(&self.tenant_limits(tenant), tenant, batch.rows())
            .is_err()
        {
            return Ok(WriteOutcome::Rejected);
        }
        let now = now_ns();
        let mut records = Vec::new();
        for series in 0..batch.series {
            let mut trace_id = [0_u8; 16];
            trace_id[..8].copy_from_slice(&batch.seq.to_be_bytes());
            trace_id[8..12].copy_from_slice(&series.to_be_bytes());
            for point in 0..batch.samples {
                let mut span_id = [0_u8; 8];
                span_id[..4].copy_from_slice(&series.to_be_bytes());
                span_id[4..].copy_from_slice(&point.to_be_bytes());
                let duration_ns =
                    i64::try_from(mix(self.seed, batch.seq, series, point) % 1_000_000)
                        .unwrap_or(0)
                        + 1;
                records.push(SpanRecord {
                    tenant: tenant.to_string(),
                    span: Span {
                        trace_id,
                        span_id,
                        parent_span_id: None,
                        name: "GET /".into(),
                        kind: SpanKind::Server,
                        start_ns: now - i64::from(batch.samples - point) * 1_000,
                        duration_ns,
                        status: StatusCode::Ok,
                        status_message: String::new(),
                        resource_attrs: vec![KeyValue {
                            key: "service.name".into(),
                            value: AttrValue::Str("api".into()),
                        }],
                        span_attrs: Vec::new(),
                        events: Vec::new(),
                        links: Vec::new(),
                        instrumentation_scope: "soak".into(),
                        instrumentation_version: String::new(),
                    },
                });
            }
        }
        let mut index = TraceIndex::new();
        build_blocks(
            &BlockWriter::new(Arc::clone(&self.stores.write)),
            &mut index,
            tenant,
            0,
            &records,
            (batch.seq, batch.seq),
        )
        .await
        .map_err(err)?;
        index
            .save_latest_snapshot(&self.stores.write, INDEX_KEY)
            .await
            .map_err(err)?;
        Ok(WriteOutcome::Accepted)
    }

    async fn query(&self, tenant: &str, window: Duration, fresh: bool) -> Result<u64, String> {
        let engine = self.engine(fresh).await?;
        let end = now_ns();
        let response = engine
            .search(tenant, "{ }", end - window_ns(window), end, SEARCH_LIMIT)
            .await
            .map_err(err)?;
        u64::try_from(response.traces.len()).map_err(err)
    }

    async fn compact(&self) -> Result<Value, String> {
        let store = Arc::clone(&self.stores.maintenance);
        self.with_compactor_index(async |index| {
            let outcome = compact_once(
                Arc::clone(&store),
                &BlockWriter::new(Arc::clone(&store)),
                index,
                "",
                CompactionPolicy::new(
                    8,
                    usize::MAX,
                    BlockLevel(4),
                    days(36_500),
                    BlockTimestampUnit::Nanos,
                ),
            )
            .await
            .map_err(err)?;
            let report = json!({
                "outputs": outcome.outputs.len(),
                "inputs_retired": outcome.retired_inputs.len(),
            });
            Ok((outcome.retired_inputs, report))
        })
        .await
    }

    async fn expire(&self, retention_secs: u32) -> Result<Value, String> {
        let overrides = OverridesProvider::from_yaml(&format!(
            "overrides:\n  soak:\n    block_retention: {retention_secs}s\n  \
             quiet:\n    block_retention: {retention_secs}s\n  \
             noisy:\n    block_retention: {retention_secs}s\n"
        ))
        .map_err(err)?;
        self.with_compactor_index(async |index| {
            let expired = expire_trace_blocks(index, UnixNano(now_ns()), &overrides);
            let report = json!({ "expired": expired.len() });
            Ok((
                expired.into_iter().map(|block| block.object_key).collect(),
                report,
            ))
        })
        .await
    }

    fn limit(&self, tenant: &str, rows_per_sec: u32, burst_rows: u64) {
        self.limits
            .lock()
            .expect("the limits lock is not poisoned")
            .insert(
                tenant.to_string(),
                Limits {
                    ingestion_rate: per_sec(rows_per_sec),
                    ingestion_burst_spans: burst_rows,
                    ..Limits::default()
                },
            );
    }

    fn rate_limiter(&self) -> Value {
        json!({
            "status": "applied",
            "detail": "krabka_traces::limits::IngestEnforcer::check_span_rate, the \
                       distributor's per-tenant token bucket; unlimited unless a phase sets a limit",
        })
    }
}
