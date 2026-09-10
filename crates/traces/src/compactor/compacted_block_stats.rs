use super::{
    BTreeMap, BTreeSet, BlockMeta, RecordBatch, ShardedTraceBloom, TraceBlockStats, TracesError,
    push_tag_metadata, push_trace_ids,
};

/// What a compacted block's index entry needs, folded over the batches as they
/// are written rather than taken from the finished block.
///
/// Every field here is a union or a set over rows, which is why a compaction
/// can build them one merged batch at a time and never hold the block it is
/// writing -- let alone the blocks it is reading.
pub(crate) struct CompactedBlockStats {
    tag_names: BTreeSet<String>,
    tag_values: BTreeMap<String, BTreeSet<String>>,
    traces: BTreeSet<[u8; 16]>,
}

impl CompactedBlockStats {
    pub(crate) fn new() -> Self {
        Self {
            tag_names: BTreeSet::new(),
            tag_values: BTreeMap::new(),
            traces: BTreeSet::new(),
        }
    }

    /// Folds one written batch into the entry.
    ///
    /// # Errors
    /// Returns [`TracesError::Block`] when the batch is not shaped like a span
    /// block.
    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<(), TracesError> {
        push_tag_metadata(batch, &mut self.tag_names, &mut self.tag_values)?;
        push_trace_ids(batch, &mut self.traces)
    }

    /// The index entry for the block `meta` describes.
    pub(crate) fn into_block_stats(self, meta: &BlockMeta) -> TraceBlockStats {
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(self.traces.len());
        for trace_id in &self.traces {
            bloom.insert(trace_id);
        }
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom,
            tag_names: self.tag_names,
            tag_values: self.tag_values,
            row_count: meta.row_count,
            // What the writer stamped. `replace_trace_blocks` promotes it
            // against the blocks this one replaces.
            level: meta.level,
        }
    }
}
