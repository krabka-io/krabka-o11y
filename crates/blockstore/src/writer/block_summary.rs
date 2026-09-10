use super::{
    Array, BTreeSet, BlockStoreError, FixedSizeBinaryArray, Int64Array, RecordBatch, Result,
    SeriesFingerprint, SummaryColumns, UInt64Array,
};

/// The [`BlockMeta`](crate::BlockMeta) summary of the batches seen so far.
///
/// A block's time bounds, row count and fingerprint set are folds over its
/// rows, so they accumulate as the rows go past and never need the whole block
/// resident. [`super::summarize`] is this fold run over a slice; the streaming
/// writer is the same fold run one batch at a time.
///
/// Getting this wrong is quiet rather than loud: `min_ts` and `max_ts` are what
/// block pruning tests a query's time range against, so a bound that is too
/// narrow drops live rows from the answer with no error anywhere.
pub(crate) struct BlockSummary {
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    fingerprints: BTreeSet<SeriesFingerprint>,
}

impl BlockSummary {
    pub(crate) fn new() -> Self {
        Self {
            // Identities for the two folds, so the first row of the block
            // decides both bounds however it compares.
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            row_count: 0,
            fingerprints: BTreeSet::new(),
        }
    }

    /// Folds `batch` into the running summary.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when `batch` lacks a summary
    /// column or types one of them differently from the declaration.
    pub(crate) fn push(&mut self, batch: &RecordBatch, summary: &SummaryColumns) -> Result<()> {
        self.row_count += batch.num_rows();

        let ts = batch
            .column_by_name(&summary.ts_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing timestamp column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlockStoreError::InvalidBlock("timestamp not Int64".into()))?;
        let id = batch
            .column_by_name(&summary.id_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing identity column".into()))?;
        // Only the series/logs/metrics (`UInt64` fingerprint) path populates
        // `BlockMeta.fingerprints`; trace (span) blocks key the identity column
        // as `FixedSizeBinary` and never read `fingerprints`, so we skip the
        // per-row FNV pass entirely for them. `FixedSizeBinary` is still an
        // accepted id-column type — we just don't fingerprint it.
        let id_u64 = id.as_any().downcast_ref::<UInt64Array>();
        if id_u64.is_none() && id.as_any().downcast_ref::<FixedSizeBinaryArray>().is_none() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be UInt64 or FixedSizeBinary",
                summary.id_col
            )));
        }

        for row in 0..batch.num_rows() {
            if !ts.is_null(row) {
                let at = ts.value(row);
                self.min_ts = self.min_ts.min(at);
                self.max_ts = self.max_ts.max(at);
            }
            if let Some(fingerprints) = id_u64
                && !fingerprints.is_null(row)
            {
                self.fingerprints.insert(fingerprints.value(row));
            }
        }

        Ok(())
    }

    /// The summary of every batch pushed so far.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when no row was ever pushed:
    /// a block with no rows has no time bounds to prune by and nothing to
    /// read, and writing one would leave an index entry no query can use.
    pub(crate) fn finish(self) -> Result<(i64, i64, usize, Vec<SeriesFingerprint>)> {
        if self.row_count == 0 {
            return Err(BlockStoreError::InvalidBlock("empty block".into()));
        }
        Ok((
            self.min_ts,
            self.max_ts,
            self.row_count,
            self.fingerprints.into_iter().collect(),
        ))
    }
}
