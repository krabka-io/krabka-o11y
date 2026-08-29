#[derive(Debug, Error)]
pub enum KafkaWalCompactionError {
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
}

#[derive(Debug, Error)]
pub enum CompactorRunError {
    #[error(transparent)]
    Wal(#[from] KafkaWalCompactionError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("compacted WAL batch did not report a commit position")]
    MissingCommitPosition,
}

#[derive(Debug, Error)]
pub enum CompactionFrontierStoreError {
    #[error("invalid compaction frontier manifest version {actual}; expected {expected}")]
    InvalidVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_next_kafka_wal_batch_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
) -> Result<Option<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    if records.is_empty() {
        return Ok(None);
    }

    let mut committer = LastCompactedPosition::default();
    let descriptor = compact_kafka_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        &mut committer,
        records,
    )
    .await?;
    let position = committer
        .position
        .ok_or(CompactorRunError::MissingCommitPosition)?;
    consumer.commit_compacted(position).await?;

    Ok(Some(descriptor))
}

#[derive(Default)]
struct LastCompactedPosition {
    position: Option<WalPosition>,
}

impl CompactionOffsetCommitter for LastCompactedPosition {
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError> {
        self.position = Some(position);
        Ok(())
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_kafka_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<KafkaWalRecord>,
) -> Result<BlockDescriptor, KafkaWalCompactionError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(compact_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        decoded,
    )
    .await?)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
) -> Result<BlockDescriptor, CompactionError> {
    compact_wal_records_to_object_store_with_delete_filters_and_index_output(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        records,
        (&[], LogCompactionIndexOutput::FullManifestAndShardCatalog),
    )
    .await?
    .ok_or(CompactionError::AllRowsDeleted)
}

async fn compact_wal_records_to_object_store_with_delete_filters_and_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
    output: (&[ActiveLogDeleteFilter], LogCompactionIndexOutput),
) -> Result<Option<BlockDescriptor>, CompactionError> {
    let (delete_filters, index_output) = output;
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let tenant = first.tenant.clone();
    let first_position = first.position.ok_or(CompactionError::MissingWalPosition {
        timestamp_ns: first.timestamp_ns,
    })?;
    let partition = first_position.partition;
    let mut first_offset = first_position.offset;
    let mut last_offset = first_position.offset;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    let mut staged_label_index = label_index.clone();
    let mut rows = Vec::with_capacity(records.len());

    for record in records {
        if record.tenant != tenant {
            return Err(CompactionError::MixedTenant {
                expected: tenant,
                actual: record.tenant,
            });
        }
        let position = record.position.ok_or(CompactionError::MissingWalPosition {
            timestamp_ns: record.timestamp_ns,
        })?;
        if position.partition != partition {
            return Err(CompactionError::MixedPartition {
                expected: partition.get(),
                actual: position.partition.get(),
            });
        }

        first_offset = first_offset.min(position.offset);
        last_offset = last_offset.max(position.offset);
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
        if is_deleted_log_entry(
            delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) {
            continue;
        }
        let fingerprint = staged_label_index.insert_series(&tenant, record.labels);
        rows.push(LogRow::new(
            fingerprint,
            record.timestamp_ns,
            record.line,
            record.structured_metadata,
        ));
    }

    if rows.is_empty() {
        committer.commit_compacted(WalPosition {
            partition,
            offset: last_offset,
        })?;
        return Ok(None);
    }

    let key = BlockKey::new(
        tenant,
        partition.get(),
        first_offset.get(),
        last_offset.get(),
        TimeRange::new(start_ns, end_ns)?,
    );
    let mut staged_block_index = block_index.clone();
    let descriptor = compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        &key,
        &staged_label_index,
        &mut staged_block_index,
        rows,
        index_output,
    )
    .await?;

    committer.commit_compacted(WalPosition {
        partition,
        offset: last_offset,
    })?;
    *label_index = staged_label_index;
    *block_index = staged_block_index;

    Ok(Some(descriptor))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalPosition {
    pub partition: PartitionIndex,
    pub offset: Offset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalLogRecord {
    pub tenant: String,
    pub labels: Labels,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: BTreeMap<String, String>,
    pub position: Option<WalPosition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalRecord {
    pub value: Vec<u8>,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<KafkaWalHeader>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionFrontier {
    pub compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl CompactionFrontier {
    #[must_use]
    pub fn new(compacted_through_ns: i64) -> Self {
        Self {
            compacted_through_ns,
            partition_offsets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_partition_offset(mut self, partition: PartitionIndex, offset: Offset) -> Self {
        self.partition_offsets.insert(partition, offset);
        self
    }

    pub fn advance_partition_offset(&mut self, position: WalPosition) {
        self.partition_offsets
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
    }

    fn is_compacted(&self, record: &WalLogRecord) -> bool {
        if let Some(position) = record.position
            && self
                .partition_offsets
                .get(&position.partition)
                .is_some_and(|offset| position.offset <= *offset)
        {
            return true;
        }

        record.timestamp_ns <= self.compacted_through_ns
    }
}

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    frontier: Arc<Mutex<CompactionFrontier>>,
}

impl SharedCompactionFrontier {
    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(frontier)),
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn snapshot(&self) -> CompactionFrontier {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn advance_partition_offset(&self, position: WalPosition) {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .advance_partition_offset(position);
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn replace(&self, frontier: CompactionFrontier) {
        *self.frontier.lock().expect("frontier mutex poisoned") = frontier;
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}

const COMPACTION_FRONTIER_MANIFEST_VERSION: u32 = 1;
const COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH: &str = "index/logs/compaction-frontier.json";

#[derive(Deserialize, Serialize)]
struct CompactionFrontierManifest {
    version: u32,
    compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl From<&CompactionFrontier> for CompactionFrontierManifest {
    fn from(frontier: &CompactionFrontier) -> Self {
        Self {
            version: COMPACTION_FRONTIER_MANIFEST_VERSION,
            compacted_through_ns: frontier.compacted_through_ns,
            partition_offsets: frontier.partition_offsets.clone(),
        }
    }
}

impl TryFrom<CompactionFrontierManifest> for CompactionFrontier {
    type Error = CompactionFrontierStoreError;

    fn try_from(manifest: CompactionFrontierManifest) -> Result<Self, Self::Error> {
        if manifest.version != COMPACTION_FRONTIER_MANIFEST_VERSION {
            return Err(CompactionFrontierStoreError::InvalidVersion {
                actual: manifest.version,
                expected: COMPACTION_FRONTIER_MANIFEST_VERSION,
            });
        }

        Ok(Self {
            compacted_through_ns: manifest.compacted_through_ns,
            partition_offsets: manifest.partition_offsets,
        })
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn write_compaction_frontier_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &CompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    let payload = serde_json::to_vec_pretty(&CompactionFrontierManifest::from(frontier))?;
    store
        .put(
            &compaction_frontier_manifest_object_path(prefix),
            payload.into(),
        )
        .await?;
    Ok(())
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn read_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<CompactionFrontier, CompactionFrontierStoreError> {
    let bytes = store
        .get(&compaction_frontier_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: CompactionFrontierManifest = serde_json::from_slice(&bytes)?;
    manifest.try_into()
}

fn compaction_frontier_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}

#[derive(Clone, Debug)]
enum CompactionFrontierSource {
    Snapshot(CompactionFrontier),
    Shared(SharedCompactionFrontier),
}

impl CompactionFrontierSource {
    fn snapshot(&self) -> CompactionFrontier {
        match self {
            Self::Snapshot(frontier) => frontier.clone(),
            Self::Shared(frontier) => frontier.snapshot(),
        }
    }
}

struct ConfiguredObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

type CompactionFrontierRefreshSource = (Arc<dyn ObjectStore>, ObjectPath);

