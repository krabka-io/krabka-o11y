//! Logs: the write-ahead compactor and its tenant index manifest, block merges
//! and retention through that manifest, and `LogQL` over the object store.
//!
//! The logs compactor is the one writer of a tenant's index, so the harness
//! holds each tenant's index in memory and serializes the writers, the merge
//! and the retention pass on it, as that one process does.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use krabka_blockstore::{
    BlockDeletion, BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex, LogBlockStoreError,
    TimeRange, delete_blocks, labels, log_block_object_path, read_log_block_from_object_store,
    read_tenant_log_index_manifest_from_object_store,
    write_tenant_log_index_manifest_to_object_store,
};
use krabka_logql::{parse_query, plan_stream_query};
use krabka_observability::{
    CompactionCommitError, CompactionOffsetCommitter, Offset, PartitionIndex, WalLogRecord,
    WalPosition, compact_log_block_to_object_store, compact_wal_records_to_object_store,
    execute_stream_query_from_object_store,
};
use object_store::path::Path as ObjectPath;
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    Batch, Signal, Stores, TENANTS, WriteOutcome, config::mix, due, err, now_ns, window_ns,
};

const PREFIX: &str = "observability/logs";
const QUERY: &str = r#"{app="api"}"#;

/// How many blocks one merge folds together.
const MERGE_BLOCKS: usize = 8;

/// Accepts every commit. No broker is in the path, so there is no offset to
/// advance; see the `wal_lag` entry in the report.
struct NoBroker;

impl CompactionOffsetCommitter for NoBroker {
    fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), CompactionCommitError> {
        Ok(())
    }
}

#[derive(Default)]
struct TenantIndex {
    labels: LabelIndex,
    blocks: LogBlockIndex,
}

type Cached = (Instant, LabelIndex, LogBlockIndex);

pub struct LogsSignal {
    stores: Stores,
    seed: u64,
    prefix: ObjectPath,
    writer: HashMap<&'static str, AsyncMutex<TenantIndex>>,
    reader: AsyncMutex<HashMap<String, Cached>>,
}

impl LogsSignal {
    /// Loads every tenant's index manifest, as the compactor does on start.
    pub async fn open(stores: Stores, seed: u64) -> Result<Self, String> {
        let prefix = ObjectPath::from(PREFIX);
        let mut writer = HashMap::new();
        for tenant in TENANTS {
            let (labels, blocks) = load(&stores.write, &prefix, tenant).await?;
            writer.insert(tenant, AsyncMutex::new(TenantIndex { labels, blocks }));
        }
        Ok(Self {
            stores,
            seed,
            prefix,
            writer,
            reader: AsyncMutex::new(HashMap::new()),
        })
    }

    fn index(&self, tenant: &str) -> Result<&AsyncMutex<TenantIndex>, String> {
        self.writer
            .get(tenant)
            .ok_or_else(|| format!("tenant {tenant} is not one of the soak's tenants"))
    }

    /// Drops `dropped` from `tenant`'s index, publishes the index, and only
    /// then deletes the objects, so no published index names a missing block.
    async fn retire(
        &self,
        tenant: &str,
        index: &mut TenantIndex,
        dropped: &[BlockKey],
    ) -> Result<u64, String> {
        if dropped.is_empty() {
            return Ok(0);
        }
        let store = &*self.stores.maintenance;
        let mut kept = LogBlockIndex::default();
        for block in index.blocks.blocks() {
            if !dropped.contains(&block.key) {
                kept.insert(block.clone());
            }
        }
        write_tenant_log_index_manifest_to_object_store(
            store,
            &self.prefix,
            tenant,
            &index.labels,
            &kept,
        )
        .await
        .map_err(err)?;
        index.blocks = kept;
        let deletions: Vec<BlockDeletion> = dropped
            .iter()
            .map(|key| BlockDeletion {
                object_key: log_block_object_path(&self.prefix, key).to_string(),
                sidecars: Vec::new(),
            })
            .collect();
        let report = delete_blocks(store, &deletions).await;
        if !report.failures.is_empty() {
            return Err(format!("{} block deletions failed", report.failures.len()));
        }
        u64::try_from(report.blocks_deleted).map_err(err)
    }

    /// Merges the oldest [`MERGE_BLOCKS`] blocks of `tenant` into one.
    async fn merge(&self, tenant: &str) -> Result<(u64, u64), String> {
        let store = &*self.stores.maintenance;
        let mut index = self.index(tenant)?.lock().await;
        let mut inputs: Vec<BlockDescriptor> = index.blocks.blocks().to_vec();
        if inputs.len() < MERGE_BLOCKS {
            return Ok((0, 0));
        }
        inputs.sort_by_key(|block| block.key.time_range.start_ns);
        inputs.truncate(MERGE_BLOCKS);
        let mut rows = Vec::new();
        for block in &inputs {
            rows.extend(
                read_log_block_from_object_store(store, &self.prefix, &block.key)
                    .await
                    .map_err(err)?,
            );
        }
        let first = inputs.iter().map(|b| b.key.first_offset).min().unwrap_or(0);
        let last = inputs.iter().map(|b| b.key.last_offset).max().unwrap_or(0);
        let start = inputs
            .iter()
            .map(|b| b.key.time_range.start_ns)
            .min()
            .unwrap_or(0);
        let end = inputs
            .iter()
            .map(|b| b.key.time_range.end_ns)
            .max()
            .unwrap_or(0);
        let merged = BlockKey::new(
            tenant,
            0,
            first,
            last,
            TimeRange::new(start, end).map_err(err)?,
        );
        let TenantIndex { labels, blocks } = &mut *index;
        compact_log_block_to_object_store(store, &self.prefix, &merged, labels, blocks, rows)
            .await
            .map_err(err)?;
        let dropped: Vec<BlockKey> = inputs.into_iter().map(|block| block.key).collect();
        let deleted = self.retire(tenant, &mut index, &dropped).await?;
        Ok((1, deleted))
    }
}

async fn load(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<(LabelIndex, LogBlockIndex), String> {
    match read_tenant_log_index_manifest_from_object_store(&**store, prefix, tenant).await {
        Ok(indexes) => Ok(indexes),
        Err(LogBlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
            Ok((LabelIndex::default(), LogBlockIndex::default()))
        }
        Err(error) => Err(err(error)),
    }
}

/// Counts the log lines in a `/loki/api/v1/query_range`-shaped streams result.
fn lines(response: &Value) -> Result<u64, String> {
    let result = response
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("the response has no data.result: {response}"))?;
    let count = result
        .iter()
        .filter_map(|stream| stream.get("values").and_then(Value::as_array))
        .map(Vec::len)
        .sum::<usize>();
    u64::try_from(count).map_err(err)
}

#[async_trait]
impl Signal for LogsSignal {
    async fn write(&self, tenant: &str, batch: Batch) -> Result<WriteOutcome, String> {
        let now = now_ns();
        let mut records = Vec::new();
        let mut offset = batch.seq * 1_000_000;
        for series in 0..batch.series {
            let stream = series.to_string();
            for point in 0..batch.samples {
                let value = mix(self.seed, batch.seq, series, point);
                records.push(WalLogRecord {
                    tenant: tenant.to_string(),
                    labels: labels([("app", "api"), ("env", "prod"), ("series", &stream)]),
                    timestamp_ns: now - i64::from(batch.samples - point) * 1_000,
                    line: format!("level=info series={series} point={point} value={value:016x}"),
                    structured_metadata: BTreeMap::new(),
                    position: Some(WalPosition {
                        partition: PartitionIndex(0),
                        offset: Offset(offset),
                    }),
                });
                offset += 1;
            }
        }
        let mut index = self.index(tenant)?.lock().await;
        let TenantIndex { labels, blocks } = &mut *index;
        compact_wal_records_to_object_store(
            &*self.stores.write,
            &self.prefix,
            labels,
            blocks,
            &mut NoBroker,
            records,
        )
        .await
        .map_err(err)?;
        Ok(WriteOutcome::Accepted)
    }

    async fn query(&self, tenant: &str, window: Duration, fresh: bool) -> Result<u64, String> {
        let (label_index, block_index) = {
            let mut reader = self.reader.lock().await;
            let cached = reader
                .get(tenant)
                .filter(|(at, _, _)| !due(Some(*at), fresh));
            if let Some((_, labels, blocks)) = cached {
                (labels.clone(), blocks.clone())
            } else {
                let (labels, blocks) = load(&self.stores.read, &self.prefix, tenant).await?;
                reader.insert(
                    tenant.to_string(),
                    (Instant::now(), labels.clone(), blocks.clone()),
                );
                (labels, blocks)
            }
        };
        let end = now_ns();
        let range = TimeRange::new(end - window_ns(window), end).map_err(err)?;
        let plan = plan_stream_query(
            tenant,
            range,
            parse_query(QUERY).map_err(err)?,
            &label_index,
            &block_index,
        )
        .map_err(err)?;
        let response = execute_stream_query_from_object_store(
            Arc::clone(&self.stores.read),
            &self.prefix,
            &plan,
            &label_index,
        )
        .await
        .map_err(err)?;
        lines(&response)
    }

    async fn compact(&self) -> Result<Value, String> {
        let (mut outputs, mut deleted) = (0, 0);
        for tenant in TENANTS {
            let (o, d) = self.merge(tenant).await?;
            outputs += o;
            deleted += d;
        }
        Ok(json!({ "outputs": outputs, "blocks_deleted": deleted }))
    }

    async fn expire(&self, retention_secs: u32) -> Result<Value, String> {
        let cutoff = now_ns() - i64::from(retention_secs) * 1_000_000_000;
        let mut deleted = 0;
        for tenant in TENANTS {
            let mut index = self.index(tenant)?.lock().await;
            let expired: Vec<BlockKey> = index
                .blocks
                .blocks()
                .iter()
                .filter(|block| block.key.time_range.end_ns < cutoff)
                .map(|block| block.key.clone())
                .collect();
            deleted += self.retire(tenant, &mut index, &expired).await?;
        }
        Ok(json!({ "blocks_deleted": deleted }))
    }

    fn limit(&self, _tenant: &str, _rows_per_sec: u32, _burst_rows: u64) {}

    fn rate_limiter(&self) -> Value {
        json!({
            "status": "not_applied",
            "reason": "the logs ingest limiter lives in the HTTP distributor, in front of the \
                       write-ahead log this soak bypasses; rejected counts for logs are always \
                       zero",
        })
    }
}
