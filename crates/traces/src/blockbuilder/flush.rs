use super::*;

/// Decoded records from one Kafka partition and their inclusive offset range.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionWindow {
    pub offset_range: OffsetRange,
    pub records: Vec<SpanRecord>,
}


/// Accumulates decoded [`PartitionWindow`]s across several WAL polls, so the
/// block-builder can flush one larger block per partition instead of a tiny
/// block per poll.
///
/// The accumulator merges successive polls by partition. It appends their
/// records and widens the inclusive offset range to span every buffered record,
/// so the block object key is a pure function of the buffered offset range.
///
/// A crash-and-reprocess that re-forms the *same* buffer, with the same records
/// and the same flush boundary, gets an identical key, and the re-run
/// overwrites the block idempotently. Flush boundaries depend on timing,
/// through the record count and the age, so this is at-least-once delivery. It
/// does not guarantee byte-identical keys across every recovery.
#[derive(Debug, Default)]
pub struct FlushAccumulator {
    pub(crate) windows: BTreeMap<i32, PartitionWindow>,
    pub(crate) record_count: usize,
    pub(crate) oldest_record_at: Option<Instant>,
}

impl FlushAccumulator {
    /// Create an empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of span records currently buffered across all partitions.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Whether the buffer holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Merge one poll's decoded windows into the buffer.
    ///
    /// This appends records per partition and widens the inclusive offset range
    /// to cover both the buffered and the incoming records. `now` stamps the
    /// arrival time that age-based flushing uses. It is recorded only the first
    /// time records enter an otherwise-empty buffer, so the age tracks the
    /// *oldest* buffered record.
    pub fn merge(&mut self, windows: BTreeMap<i32, PartitionWindow>, now: Instant) {
        for (partition, incoming) in windows {
            if incoming.records.is_empty() {
                continue;
            }
            if self.oldest_record_at.is_none() {
                self.oldest_record_at = Some(now);
            }
            self.record_count += incoming.records.len();
            self.windows
                .entry(partition)
                .and_modify(|buffered| {
                    buffered.offset_range.min =
                        buffered.offset_range.min.min(incoming.offset_range.min);
                    buffered.offset_range.max =
                        buffered.offset_range.max.max(incoming.offset_range.max);
                    buffered.records.extend(incoming.records.iter().cloned());
                })
                .or_insert(incoming);
        }
    }

    pub fn remove_partitions(&mut self, partitions: &BTreeSet<i32>) {
        for partition in partitions {
            if let Some(window) = self.windows.remove(partition) {
                self.record_count -= window.records.len();
            }
        }
        if self.record_count == 0 {
            self.oldest_record_at = None;
        }
    }

    /// Whether the buffered records should be flushed now.
    ///
    /// This is true once the buffer reaches the record-count threshold, or once
    /// the oldest buffered record ages past `flush_max_age`. It is always false
    /// when the buffer is empty.
    #[must_use]
    pub fn should_flush(&self, config: &BlockBuilderConfig, now: Instant) -> bool {
        if self.record_count == 0 {
            return false;
        }
        if self.record_count >= config.flush_max_records {
            return true;
        }
        match self.oldest_record_at {
            Some(oldest) => now.saturating_duration_since(oldest).as_time() >= config.flush_max_age,
            None => false,
        }
    }

    /// Drain the buffered windows and reset the accumulator to empty.
    #[must_use]
    pub fn take(&mut self) -> BTreeMap<i32, PartitionWindow> {
        self.record_count = 0;
        self.oldest_record_at = None;
        std::mem::take(&mut self.windows)
    }
}


/// Flush decoded partition windows and durably save the trace index.
///
/// This function returns the number of span blocks it durably wrote. The caller
/// should commit WAL offsets only after this returns `Ok(_)`.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn flush_partition_windows(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_store: Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    windows: BTreeMap<i32, PartitionWindow>,
) -> Result<usize, TracesError> {
    let mut blocks_written = 0usize;
    for (partition, partition_window) in windows {
        for tenant in tenants_in_records(&partition_window.records) {
            let metas = build_blocks_with_options(
                writer,
                index,
                &tenant,
                partition,
                &partition_window.records,
                partition_window.offset_range,
                BlockBuildOptions {
                    object_key_prefix: &config.object_key_prefix,
                    promoted_attrs: &config.promoted_attrs,
                },
            )
            .await?;
            blocks_written += metas.len();
        }
    }
    index
        .save_latest_snapshot_with_retain(
            &object_store,
            &config.index_key,
            config.index_snapshot_retain,
        )
        .await
        .map(|_| blocks_written)
        .map_err(|err| TracesError::Block(err.to_string()))
}


/// Flush the accumulated buffer to durable blocks, then commit WAL offsets.
///
/// This drains the accumulator first, so a flush error leaves nothing
/// double-counted. `commit_sync` runs only after `flush_partition_windows`
/// reports that the blocks and the trace index are durably written.
pub(crate) async fn flush_and_commit<C>(
    consumer: &mut C,
    writer: &BlockWriter,
    index: &Arc<Mutex<TraceIndex>>,
    object_store: &Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    metrics: &ServiceMetrics,
    accumulator: &mut FlushAccumulator,
) -> Result<(), TracesError>
where
    C: WalConsumerCommit,
{
    let windows = accumulator.take();
    let blocks = {
        let mut guard = index.lock().await;
        flush_partition_windows(
            writer,
            &mut guard,
            Arc::clone(object_store),
            config,
            windows,
        )
        .await?
    };
    // One counter tick per durably-written span block (post-flush, pre-commit).
    for _ in 0..blocks {
        metrics.record_block_flushed();
    }
    consumer.commit_sync().await
}

