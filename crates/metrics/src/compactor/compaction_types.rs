//! Shared compaction records, options, errors, and consumer protocols.

use super::*;

/// Result of one compactor consumer poll and processing pass.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionPollResult {
    pub polled_records: usize,
    pub compacted_records: usize,
    pub batch: CompactionBatchResult,
}

/// Errors raised while committing compactor WAL offsets.
#[derive(Debug, thiserror::Error)]
pub enum CompactionCommitError {
    #[error("compaction offset commit failed: {0}")]
    Commit(String),
}

/// Errors raised by a consumer offset commit.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerCommitError {
    #[error("consumer commit failed: {0}")]
    Commit(String),
}

/// Errors raised by a consumer poll.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerPollError {
    #[error("consumer poll failed: {0}")]
    Poll(String),
    #[error("WAL drain failed: {0}")]
    Drain(#[source] krabka_client_consumer::ConsumerError),
}

/// Errors raised while adapting Kafka consumer records to compactor WAL records.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerRecordError {
    #[error(
        "metrics WAL record at partition {partition} offset {offset} has unsupported format: {message}"
    )]
    UnsupportedFormat {
        partition: PartitionIndex,
        offset: Offset,
        message: String,
    },
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },
}

/// Compaction index sidecar codec errors.
#[derive(Debug, thiserror::Error)]
pub enum CompactionIndexError {
    #[error("compaction index encode failed: {0}")]
    Encode(String),

    #[error("compaction index decode failed: {0}")]
    Decode(String),

    #[error("compaction index object-store write failed: {0}")]
    ObjectStore(String),
}

/// Errors raised while reading the `.index` manifests under the metrics prefix.
///
/// The manifests are the metrics index, so a pass that cannot read them knows
/// nothing about which blocks are live. Every caller stops rather than acts on
/// a partial view.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionManifestError {
    #[error("compaction manifest object-store operation failed: {0}")]
    ObjectStore(String),

    #[error("compaction manifest key mismatch: listed `{listed}`, manifest `{manifest}`")]
    KeyMismatch { listed: String, manifest: String },

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}

/// Errors raised by one compactor poll/process pass.
#[derive(Debug, thiserror::Error)]
pub enum CompactionPollError {
    #[error(transparent)]
    Poll(#[from] CompactionConsumerPollError),

    #[error(transparent)]
    ConsumerRecord(#[from] CompactionConsumerRecordError),

    #[error(transparent)]
    Window(#[from] CompactionWindowError),
}

/// Errors raised while deleting compacted metric objects outside retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionRetentionError {
    #[error(transparent)]
    Manifest(#[from] CompactionManifestError),

    #[error("{source}")]
    Lifecycle {
        #[source]
        source: LifecycleError,
        stats: Box<CompactionRetentionStats>,
    },
}

impl CompactionRetentionError {
    /// Work completed before a fallible orphan listing failed.
    #[must_use]
    pub fn partial_stats(&self) -> Option<&CompactionRetentionStats> {
        match self {
            Self::Lifecycle { stats, .. } => Some(stats),
            Self::Manifest(_) => None,
        }
    }
}

/// Errors raised while processing a compaction WAL window.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWindowError {
    #[error("compaction window spans multiple partitions: {first} and {second}")]
    MultiplePartitions {
        first: PartitionIndex,
        second: PartitionIndex,
    },

    #[error(transparent)]
    Wal(#[from] WalError),

    #[error(transparent)]
    Write(#[from] CompactionWriteError),

    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}

/// Errors raised while writing compacted metric blocks.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWriteError {
    #[error(transparent)]
    Encode(#[from] HistogramCodecError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}

/// Result of processing a polled compaction batch across assigned partitions.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionBatchResult {
    pub partition_results: Vec<CompactionWindowResult>,
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}

/// Monotonic clock used by the compaction loop to age the accumulation buffer.
///
/// This is an abstraction, so a test can drive flush-by-age deterministically
/// and does not need real wall-clock waits.
pub trait CompactionClock {
    fn now(&self) -> std::time::Instant;
}

/// Minimal consumer commit surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerCommit: Send + Sync {
    async fn commit_offsets_sync(
        &self,
        topic: &str,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError>;
}

/// Minimal mutable consumer commit surface for service loops that poll and commit
/// through the same handle.
#[async_trait]
pub trait CompactionConsumerCommitMut: Send {
    async fn commit_offsets_sync_mut(
        &mut self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError>;
}

/// Adapter that commits the underlying consumer after durable compaction writes.
pub struct CompactionConsumerCommitter<'a, C: ?Sized> {
    pub(crate) consumer: &'a C,
    pub(crate) topic: &'a str,
}

impl<'a, C: ?Sized> CompactionConsumerCommitter<'a, C> {
    #[must_use]
    pub const fn new(consumer: &'a C, topic: &'a str) -> Self {
        Self { consumer, topic }
    }
}

#[async_trait]
impl<C> CompactionOffsetCommitter for CompactionConsumerCommitter<'_, C>
where
    C: CompactionConsumerCommit + ?Sized,
{
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError> {
        if offsets.is_empty() {
            return Ok(());
        }
        self.consumer
            .commit_offsets_sync(self.topic, offsets)
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))
    }
}

/// Minimal consumer poll surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerPoll: Send {
    /// Partitions revoked by the rebalance applied during the last poll.
    fn take_revoked_partitions(&mut self) -> std::collections::BTreeSet<i32> {
        std::collections::BTreeSet::new()
    }

    /// Whether the final durable commits cover a frozen WAL boundary.
    ///
    /// The loop calls this only after it flushes every current buffer on shutdown.
    /// Finite test consumers have no additional records to drain.
    async fn drain_complete(&mut self) -> Result<bool, CompactionConsumerPollError> {
        Ok(true)
    }

    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError>;
}

/// Sink for compaction index sidecars.
#[async_trait]
pub trait CompactionIndexSink: Send + Sync {
    async fn write_manifest(
        &self,
        manifest: &CompactionIndexManifest,
    ) -> Result<(), CompactionIndexError>;
}

/// Commits compacted WAL offsets after block and index writes are durable.
#[async_trait]
pub trait CompactionOffsetCommitter: Send + Sync {
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError>;
}

/// Offset to commit for one compacted WAL partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset. This is the next offset after the last durable
    /// record.
    pub offset: Offset,
}

/// Runtime knobs for the compactor polling loop.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionLoopConfig {
    pub wal_topic: String,
    pub poll_timeout: Time,
    /// Flush the accumulated buffer once this many WAL records are buffered.
    pub flush_max_rows: usize,
    /// Flush the accumulated buffer once its oldest record reaches this age.
    pub flush_max_age: Time,
}

/// The collaborators a compactor loop consults on every iteration but does not
/// read its work from: the clock it measures flush age with, and the
/// instruments it records the poll and the flush into.
///
/// They travel together because they are the loop's ambient context rather
/// than its inputs. A test injects a clock that does not advance on its own and
/// a registry it can scrape, and it injects both the same way.
#[derive(Clone, Copy)]
pub struct CompactionLoopContext<'a, Clock: ?Sized> {
    /// The clock the loop reads to decide whether the buffer has aged out.
    pub clock: &'a Clock,
    /// The instruments the loop records its polls and its flushes into.
    pub metrics: &'a ServiceMetrics,
}

impl<'a, Clock> CompactionLoopContext<'a, Clock>
where
    Clock: CompactionClock + ?Sized,
{
    /// Builds a context from a clock and an instrument bundle.
    #[must_use]
    pub fn new(clock: &'a Clock, metrics: &'a ServiceMetrics) -> Self {
        Self { clock, metrics }
    }
}

/// Summary returned after a compactor loop exits.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionLoopResult {
    pub polls: usize,
    pub polled_records: usize,
    pub compacted_records: usize,
    pub writes: usize,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}

/// One series label set persisted in a compaction manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionSeriesLabels {
    pub fingerprint: u64,
    pub labels: crate::MetricLabels,
}

/// One encoded WAL record fetched by the compactor for a single topic partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionWalRecord {
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Vec<u8>,
}

/// Result of processing one partition's compaction window.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionWindowResult {
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offset: Option<CompactionPartitionOffset>,
}

pub(crate) struct CompactedBlockRequest<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) kind: MetricBlockKind,
    pub(crate) partition: Option<PartitionIndex>,
    pub(crate) first_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) batch: RecordBatch,
    pub(crate) series: Vec<CompactionSeriesLabels>,
}

/// One persisted metric block and its committed index sidecar description.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactedBlockWrite {
    pub kind: MetricBlockKind,
    pub block_meta: BlockMeta,
    pub manifest: CompactionIndexManifest,
}

/// One merge the metrics planner asks for.
///
/// The kind travels with the job because a metric block holds one payload kind
/// and one schema. Float rows and native-histogram rows share a fingerprint
/// column and nothing else, so a job that mixed two kinds would have no output
/// schema to write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricCompactionJob {
    /// The payload kind of every input, and of the output.
    pub kind: MetricBlockKind,
    /// The blocks to merge, their level, and the range they cover.
    pub job: CompactionJob,
}

/// What one pass of
/// [`compact_metric_blocks_once`](super::compact_metric_blocks_once) did.
///
/// The three parts are in the order the pass did them: it writes the merged
/// blocks and publishes their manifests, it retires the inputs' manifests, and
/// it deletes the block objects that an *earlier* pass retired. The last part
/// belongs to the earlier pass's inputs and not to this one's, which is the
/// point of deferring it. See [`DeferredBlockDeletions`](super::DeferredBlockDeletions).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricCompactionPass {
    /// The manifests of the merged blocks, one per planned job.
    pub outputs: Vec<CompactionIndexManifest>,
    /// What the retirement of the inputs' `.index` manifests did.
    pub manifests_retired: CompactionRetentionPhase,
    /// What the deletion of an earlier pass's retired blocks did.
    pub blocks_deleted: CompactionRetentionPhase,
}

impl MetricCompactionPass {
    /// Whether the pass merged nothing and deleted nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
            && self.manifests_retired.deleted == 0
            && self.blocks_deleted.deleted == 0
    }
}

/// Metric block payload kind used in deterministic object keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MetricBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}

impl MetricBlockKind {
    pub(crate) const fn object_path(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::NativeHistograms => "native-histograms",
            Self::Exemplars => "exemplars",
            Self::Metadata => "metadata",
            Self::ClockReadings => "clock-readings",
        }
    }

    /// Whether repeated `(fingerprint, timestamp)` rows are duplicates.
    ///
    /// The other kinds preserve repeats: exemplars may legitimately share that
    /// pair, metadata is set-deduplicated by its read path, and clock readings
    /// are an archival record beside their queryable float projection.
    pub(crate) const fn deduplicates_series_timestamp(self) -> bool {
        matches!(self, Self::Float | Self::NativeHistograms)
    }
}

/// Errors raised while merging metric blocks that are already in object storage.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetricCompactionError {
    #[error(transparent)]
    Manifest(#[from] CompactionManifestError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Codec(#[from] HistogramCodecError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// A job named an input the index does not hold.
    ///
    /// The planner reads the same manifests the merge does, so this is a fault
    /// in the compactor rather than in the bucket.
    #[error("compaction input `{object_key}` has no index manifest")]
    UnknownInput { object_key: String },

    /// Two inputs of one job do not carry the same Arrow schema.
    ///
    /// A merge writes one block, so its inputs must agree on the columns. Each
    /// metric block kind has exactly one schema, so two that disagree were not
    /// written by the same code.
    #[error("compaction inputs disagree on their schema: `{first}` and `{second}`")]
    SchemaMismatch { first: String, second: String },
}

/// Errors raised while constructing live compactor role dependencies.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorBuildError {
    #[error(transparent)]
    Config(#[from] MetricsCompactorConfigError),

    #[error("metrics compactor consumer build failed: {0}")]
    Consumer(String),
}

/// Errors raised while configuring the metrics compactor role.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorConfigError {
    #[error("metrics compactor config `{field}` must not be empty")]
    Empty { field: &'static str },

    #[error("metrics compactor poll_timeout must be non-zero")]
    ZeroPollTimeout,

    #[error("metrics compactor flush_max_rows must be non-zero")]
    ZeroFlushMaxRows,
}

/// Runtime handles assembled for the compactor role.
pub struct MetricsCompactorRuntime {
    pub block_writer: BlockWriter,
    pub index_sink: ObjectStoreCompactionIndexSink,
    pub loop_config: CompactionLoopConfig,
}

/// One sorted float sample row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct FloatRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}

/// One metric metadata row ready for indexing.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetadataRow {
    pub fingerprint: u64,
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// One sorted native histogram row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogramRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub hist: NativeHistogram,
}

/// One sorted clock confidence row ready for block encoding.
///
/// The row keeps the whole [`ClockReadingPayload`] rather than a flattened copy
/// of its two dozen fields, so the block encoder and the WAL payload can never
/// drift apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockReadingRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub reading: ClockReadingPayload,
}

/// Arrow batches produced from one tenant's compacted rows.
pub struct TenantBatches {
    pub float: Option<RecordBatch>,
    pub native_histograms: Option<RecordBatch>,
    pub exemplars: Option<RecordBatch>,
    pub metadata: Option<RecordBatch>,
    pub clock_readings: Option<RecordBatch>,
}

/// Compacted rows for a single tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantCompactionRows {
    pub tenant: String,
    pub series_labels: BTreeMap<u64, crate::MetricLabels>,
    pub float_rows: Vec<FloatRow>,
    pub histogram_rows: Vec<NativeHistogramRow>,
    pub exemplar_rows: Vec<ExemplarRow>,
    pub metadata_rows: Vec<MetadataRow>,
    pub clock_rows: Vec<ClockReadingRow>,
}

impl TenantCompactionRows {
    pub(crate) fn empty(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            series_labels: BTreeMap::new(),
            float_rows: Vec::new(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        }
    }
}

/// Real monotonic clock backed by [`std::time::Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCompactionClock;

impl CompactionClock for SystemCompactionClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}
