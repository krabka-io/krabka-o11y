//! Profiles: the block builder and its index snapshot, the lifecycle pass
//! (merge, expire, save, delete), and flame graphs from the cold store.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use krabka_blockstore::{
    BlockIndex as _, BlockLevel, BlockTimestampUnit, CompactionPolicy, DEFAULT_INDEX_SNAPSHOT_MAX,
    IndexSnapshotRetain, Labels, ObjectStoreMetrics, ProfileIndex,
};
use krabka_pprof::{EngineOpts, FlameEngine};
use krabka_profiles::{
    ProfileRecord, WalFunction, WalLocation, WalSample, WalSymbolSet,
    blockbuilder::{BLOCK_OBJECT_PREFIX, STACKTRACE_PARTITION, build_block},
    cold_store::ColdProfileStore,
    lifecycle::{LifecycleOptions, run_lifecycle_pass},
    limits::{Limits, OverridesProvider},
};
use krabka_units::hours;
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use super::{Batch, Signal, Stores, WriteOutcome, config::mix, due, err, now_ms};

const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const INDEX_KEY: &str = "index/profiles.json";
const SELECTOR: &str = r#"{service_name="api"}"#;

pub struct ProfilesSignal {
    stores: Stores,
    seed: u64,
    reader: AsyncMutex<Option<(Instant, Arc<FlameEngine<ColdProfileStore>>)>>,
    /// Serializes lifecycle passes, as the compactor's one loop does.
    compactor: AsyncMutex<()>,
}

impl ProfilesSignal {
    pub fn open(stores: Stores, seed: u64) -> Self {
        Self {
            stores,
            seed,
            reader: AsyncMutex::new(None),
            compactor: AsyncMutex::new(()),
        }
    }

    async fn load(store: &Arc<dyn object_store::ObjectStore>) -> Result<ProfileIndex, String> {
        ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            store,
            INDEX_KEY,
            DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await
        .map_err(err)
    }

    async fn engine(&self, fresh: bool) -> Result<Arc<FlameEngine<ColdProfileStore>>, String> {
        let mut reader = self.reader.lock().await;
        if let Some((at, engine)) = reader.as_ref()
            && !due(Some(*at), fresh)
        {
            return Ok(Arc::clone(engine));
        }
        let index = Self::load(&self.stores.read).await?;
        let cold = ColdProfileStore::new(Arc::clone(&self.stores.read), Arc::new(index))
            .with_index_snapshot(INDEX_KEY.to_string(), DEFAULT_INDEX_SNAPSHOT_MAX);
        let engine = Arc::new(FlameEngine::new(Arc::new(cold), EngineOpts::default()));
        *reader = Some((Instant::now(), Arc::clone(&engine)));
        Ok(engine)
    }

    async fn lifecycle(&self, overrides: &OverridesProvider, merge: bool) -> Result<Value, String> {
        let _serial = self.compactor.lock().await;
        let store = &self.stores.maintenance;
        let mut index = Self::load(store).await?;
        // The fan-in is a cap, not a minimum: the planner also closes a
        // partial run. A one-row target makes every existing block ineligible
        // when this pass is retention alone.
        let target_rows = if merge { usize::MAX } else { 1 };
        let report = run_lifecycle_pass(
            store,
            &mut index,
            &LifecycleOptions {
                index_key: INDEX_KEY,
                index_snapshot_retain: IndexSnapshotRetain::default(),
                policy: CompactionPolicy::new(
                    8,
                    target_rows,
                    BlockLevel(4),
                    hours(2),
                    BlockTimestampUnit::Millis,
                ),
                downsample: None,
                retention: overrides,
                block_prefix: BLOCK_OBJECT_PREFIX,
                orphan_grace: hours(1),
                now: SystemTime::now(),
            },
        )
        .await
        .map_err(err)?;
        if !report.deletions.failures.is_empty() {
            return Err(format!(
                "{} block deletions failed",
                report.deletions.failures.len()
            ));
        }
        Ok(json!({
            "outputs": report.compacted.len(),
            "expired": report.expired,
            "blocks_deleted": report.deletions.blocks_deleted,
        }))
    }
}

#[async_trait]
impl Signal for ProfilesSignal {
    async fn write(&self, tenant: &str, batch: Batch) -> Result<WriteOutcome, String> {
        let now = now_ms();
        let store = &self.stores.write;
        let mut index = ProfileIndex::new();
        let mut records = Vec::new();
        for series in 0..batch.series {
            let function = format!("fn_{series}");
            let record = ProfileRecord {
                tenant: tenant.to_string(),
                labels: vec![
                    ("__name__".into(), "process_cpu".into()),
                    ("__profile_type__".into(), PROFILE_TYPE.into()),
                    ("service_name".into(), "api".into()),
                    ("series".into(), series.to_string()),
                ],
                profile_type: PROFILE_TYPE.into(),
                samples: (0..batch.samples)
                    .map(|point| WalSample {
                        stacktrace_location_refs: vec![0],
                        value: i64::try_from(mix(self.seed, batch.seq, series, point) % 1_000)
                            .unwrap_or(1)
                            + 1,
                        timestamp_ns: (now - i64::from(batch.samples - point)) * 1_000_000,
                        span_id: None,
                        trace_id: None,
                    })
                    .collect(),
                symbols: WalSymbolSet {
                    strings: vec![String::new(), function],
                    functions: vec![WalFunction {
                        name: 1,
                        system_name: 1,
                        filename: 0,
                        start_line: 0,
                    }],
                    locations: vec![WalLocation {
                        address: 0,
                        mapping_id: 0,
                        lines: vec![(0, 1)],
                    }],
                    mappings: Vec::new(),
                },
            };
            let labels = Labels::from_pairs(record.labels.iter().cloned());
            index
                .add_series(tenant, labels.fingerprint(), &labels)
                .map_err(err)?;
            records.push(record);
        }
        // One batch is one write-ahead-log window, and the block builder
        // writes one window as one block.
        for meta in build_block(
            store,
            tenant,
            0,
            &records,
            (batch.seq, batch.seq),
            &ObjectStoreMetrics::unregistered(),
        )
        .await
        .map_err(err)?
        {
            index.add_block(&meta);
            index.add_profile_block(tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
        }
        index
            .save_latest_snapshot_with_retain(store, INDEX_KEY, IndexSnapshotRetain::default())
            .await
            .map_err(err)?;
        Ok(WriteOutcome::Accepted)
    }

    async fn query(&self, tenant: &str, window: Duration, fresh: bool) -> Result<u64, String> {
        let engine = self.engine(fresh).await?;
        let end = now_ms();
        let start = end - i64::try_from(window.as_millis()).map_err(err)?;
        let graph = engine
            .select_merge_stacktraces(tenant, PROFILE_TYPE, SELECTOR, start, end, 0)
            .await
            .map_err(err)?;
        u64::try_from(graph.total).map_err(err)
    }

    async fn compact(&self) -> Result<Value, String> {
        self.lifecycle(&OverridesProvider::new(Limits::default()), true)
            .await
    }

    async fn expire(&self, retention_secs: u32) -> Result<Value, String> {
        let overrides = OverridesProvider::from_yaml(&format!(
            "overrides:\n  soak:\n    compactor_blocks_retention_period_secs: {retention_secs}\n  \
             quiet:\n    compactor_blocks_retention_period_secs: {retention_secs}\n  \
             noisy:\n    compactor_blocks_retention_period_secs: {retention_secs}\n"
        ))
        .map_err(err)?;
        self.lifecycle(&overrides, false).await
    }

    fn limit(&self, _tenant: &str, _rows_per_sec: u32, _burst_rows: u64) {}

    fn rate_limiter(&self) -> Value {
        json!({
            "status": "not_applied",
            "reason": "the profiles distributor's per-tenant rate limiter is not public API, so \
                       the soak cannot put it in front of the block builder; rejected counts \
                       for profiles are always zero",
        })
    }
}
