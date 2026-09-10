use super::{
    Array, FixedSizeBinaryArray, RecordBatch, SCOL_TRACE_ID, SchemaRef, TracesError, concat_batches,
};

/// Holds merged spans back until whole traces can be handed on.
///
/// `recompute_nested_sets` and `recompute_trace_level_columns` are per-trace
/// computations -- a nested-set numbering needs the whole tree, and a trace's
/// duration and root are folds over all of its spans -- so neither can run on
/// a batch that holds half of a trace. Under the declared
/// `[trace_id, start_unix_nano]` order a trace's spans are contiguous, so
/// "whole traces" is a cut in the merged stream rather than a regrouping of
/// it: everything before the first row of the last trace seen is complete, and
/// everything from it onwards might still be joined by a span the merge has
/// not reached.
///
/// That leaves the buffer holding at most one output batch plus the trace
/// straddling its end. A single trace is the one thing this cannot stream, and
/// nothing can: its spans have to meet somewhere for the tree to be numbered.
pub(crate) struct TraceGroupBuffer {
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    rows: usize,
}

impl TraceGroupBuffer {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            pending: Vec::new(),
            rows: 0,
        }
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) {
        self.rows += batch.num_rows();
        self.pending.push(batch);
    }

    /// The buffered spans whose traces are certainly complete, once at least
    /// `min_rows` have gathered.
    ///
    /// `None` while the buffer is short of `min_rows`, and also when
    /// everything it holds belongs to one still-open trace -- there is nothing
    /// to do then but keep reading.
    ///
    /// # Errors
    /// Returns [`TracesError::Block`] when the buffered batches cannot be
    /// concatenated or carry no `trace_id` column.
    pub(crate) fn take_complete(
        &mut self,
        min_rows: usize,
    ) -> Result<Option<RecordBatch>, TracesError> {
        if self.rows < min_rows {
            return Ok(None);
        }
        let gathered = self.gather()?;
        let cut = last_trace_start(&gathered)?;
        if cut == 0 {
            // One trace, still open. Keep it whole and wait for its end.
            self.pending = vec![gathered];
            return Ok(None);
        }
        self.rows = gathered.num_rows() - cut;
        self.pending = vec![gathered.slice(cut, self.rows)];
        Ok(Some(gathered.slice(0, cut)))
    }

    /// Everything left once the merge is spent, at which point the last trace
    /// is complete too.
    ///
    /// # Errors
    /// Returns [`TracesError::Block`] when the buffered batches cannot be
    /// concatenated.
    pub(crate) fn take_rest(&mut self) -> Result<Option<RecordBatch>, TracesError> {
        if self.rows == 0 {
            return Ok(None);
        }
        let gathered = self.gather()?;
        self.rows = 0;
        Ok(Some(gathered))
    }

    fn gather(&mut self) -> Result<RecordBatch, TracesError> {
        let gathered = match self.pending.as_slice() {
            [only] => only.clone(),
            many => concat_batches(&self.schema, many)
                .map_err(|err| TracesError::Block(err.to_string()))?,
        };
        self.pending.clear();
        Ok(gathered)
    }
}

/// The row the last trace in `batch` starts at.
///
/// Zero when the whole batch is one trace. The scan walks back from the end,
/// so it costs the length of that last trace rather than the length of the
/// batch.
fn last_trace_start(batch: &RecordBatch) -> Result<usize, TracesError> {
    let trace_ids = batch
        .column_by_name(SCOL_TRACE_ID)
        .ok_or_else(|| TracesError::Block("merged spans are missing trace_id".into()))?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block("trace_id is not FixedSizeBinary".into()))?;
    let last = batch.num_rows() - 1;
    let mut start = last;
    while start > 0 && same_trace(trace_ids, start - 1, last) {
        start -= 1;
    }
    Ok(start)
}

/// Whether two rows carry the same trace id, a null id matching only a null.
fn same_trace(trace_ids: &FixedSizeBinaryArray, left: usize, right: usize) -> bool {
    match (trace_ids.is_null(left), trace_ids.is_null(right)) {
        (true, true) => true,
        (false, false) => trace_ids.value(left) == trace_ids.value(right),
        _ => false,
    }
}
