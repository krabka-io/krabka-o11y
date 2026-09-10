use super::{
    AsArray, COL_FINGERPRINT, COL_TIMESTAMP, Int32Type, Int64Type, PCOL_PROFILE_TYPE,
    ProfilesError, RecordBatch, SchemaRef, UInt64Type, concat_batches,
};

/// Holds merged samples back until whole time buckets can be handed on.
///
/// Downsampling sums every sample that falls in one
/// `(series, profile type, bucket)` cell, so a batch holding half of a cell
/// would write a partial sum and then a second row for the same cell -- two
/// rows where the block should have one. The merged samples arrive in the
/// declared `[fingerprint, profile_type, timestamp]` order and rounding a
/// timestamp down preserves it, so a cell's samples are contiguous and "whole
/// cells" is a cut in the stream rather than a regrouping of it.
///
/// What stays resident is one output batch plus the cell straddling its end,
/// rather than every sample of every input.
pub(crate) struct SampleGroupBuffer {
    schema: SchemaRef,
    resolution_ns: i64,
    pending: Vec<RecordBatch>,
    rows: usize,
}

impl SampleGroupBuffer {
    pub(crate) fn new(schema: SchemaRef, resolution_ns: i64) -> Self {
        Self {
            schema,
            resolution_ns,
            pending: Vec::new(),
            rows: 0,
        }
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) {
        self.rows += batch.num_rows();
        self.pending.push(batch);
    }

    /// The buffered samples whose buckets are certainly complete, once at
    /// least `min_rows` have gathered.
    ///
    /// `None` while the buffer is short of `min_rows`, and also when
    /// everything it holds belongs to one still-open bucket.
    ///
    /// # Errors
    /// Returns [`ProfilesError::Block`] when the buffered batches cannot be
    /// concatenated or are not shaped like a profile-samples block.
    pub(crate) fn take_complete(
        &mut self,
        min_rows: usize,
    ) -> Result<Option<RecordBatch>, ProfilesError> {
        if self.rows < min_rows {
            return Ok(None);
        }
        let gathered = self.gather()?;
        let cut = last_bucket_start(&gathered, self.resolution_ns)?;
        if cut == 0 {
            self.pending = vec![gathered];
            return Ok(None);
        }
        self.rows = gathered.num_rows() - cut;
        self.pending = vec![gathered.slice(cut, self.rows)];
        Ok(Some(gathered.slice(0, cut)))
    }

    /// Everything left once the merge is spent, at which point the last bucket
    /// is complete too.
    ///
    /// # Errors
    /// Returns [`ProfilesError::Block`] when the buffered batches cannot be
    /// concatenated.
    pub(crate) fn take_rest(&mut self) -> Result<Option<RecordBatch>, ProfilesError> {
        if self.rows == 0 {
            return Ok(None);
        }
        let gathered = self.gather()?;
        self.rows = 0;
        Ok(Some(gathered))
    }

    fn gather(&mut self) -> Result<RecordBatch, ProfilesError> {
        let gathered = match self.pending.as_slice() {
            [only] => only.clone(),
            many => concat_batches(&self.schema, many)
                .map_err(|err| ProfilesError::Block(err.to_string()))?,
        };
        self.pending.clear();
        Ok(gathered)
    }
}

/// The row the last `(series, profile type, bucket)` cell in `batch` starts at.
///
/// Zero when the whole batch is one cell. The scan walks back from the end, so
/// it costs the length of that cell rather than the length of the batch.
fn last_bucket_start(batch: &RecordBatch, resolution_ns: i64) -> Result<usize, ProfilesError> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| ProfilesError::Block(format!("merged samples are missing `{name}`")))
    };
    let fingerprints = column(COL_FINGERPRINT)?
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| ProfilesError::Block(format!("`{COL_FINGERPRINT}` must be UInt64")))?;
    let timestamps = column(COL_TIMESTAMP)?
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| ProfilesError::Block(format!("`{COL_TIMESTAMP}` must be Int64")))?;
    let profile_types = column(PCOL_PROFILE_TYPE)?
        .as_dictionary_opt::<Int32Type>()
        .ok_or_else(|| {
            ProfilesError::Block(format!("`{PCOL_PROFILE_TYPE}` must be a dictionary"))
        })?;
    let profile_names = profile_types
        .values()
        .as_string_opt::<i32>()
        .ok_or_else(|| {
            ProfilesError::Block(format!("`{PCOL_PROFILE_TYPE}` must be a string dictionary"))
        })?;

    // The dictionary is shared across the batch, but two rows holding the same
    // string need not hold the same key after a concatenation, so cells are
    // compared by the name rather than by the key.
    let profile_name = |row: usize| -> Result<&str, ProfilesError> {
        let key = usize::try_from(profile_types.keys().value(row))
            .map_err(|err| ProfilesError::Block(format!("profile type key invalid: {err}")))?;
        Ok(profile_names.value(key))
    };
    let bucket = |row: usize| {
        timestamps
            .value(row)
            .div_euclid(resolution_ns)
            .saturating_mul(resolution_ns)
    };

    let last = batch.num_rows() - 1;
    let (last_fingerprint, last_bucket, last_name) =
        (fingerprints.value(last), bucket(last), profile_name(last)?);
    let mut start = last;
    while start > 0
        && fingerprints.value(start - 1) == last_fingerprint
        && bucket(start - 1) == last_bucket
        && profile_name(start - 1)? == last_name
    {
        start -= 1;
    }
    Ok(start)
}
