use super::{
    BitReader, ChunkError, HistogramChunkState, HistogramLayout, NativeHistogram, ResetHint,
    STALE_NAN_BITS, XorState, decode_histogram_samples, stale_histogram,
};

/// Decodes a Prometheus float native-histogram chunk.
///
/// The first sample stores every value as 64 raw bits. Each later sample
/// stores the count, the zero count, the sum and every bucket as a Gorilla XOR
/// against the same value in the sample before, each with its own state.
pub fn decode_float_histogram_chunk(
    chunk: &[u8],
    max_buckets: u64,
) -> Result<Vec<(i64, NativeHistogram)>, ChunkError> {
    decode_histogram_samples::<FloatState>(chunk, max_buckets)
}

struct FloatState {
    timestamp: i64,
    timestamp_delta: i64,
    /// The count, zero count and sum, in that order.
    scalars: [(f64, XorState); 3],
    positive: Vec<(f64, XorState)>,
    negative: Vec<(f64, XorState)>,
}

impl HistogramChunkState for FloatState {
    fn stale_marker() -> NativeHistogram {
        stale_histogram(true)
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn new(layout: &HistogramLayout) -> Self {
        Self {
            timestamp: 0,
            timestamp_delta: 0,
            scalars: [(0.0, XorState::default()); 3],
            positive: vec![(0.0, XorState::default()); layout.positive_buckets],
            negative: vec![(0.0, XorState::default()); layout.negative_buckets],
        }
    }

    fn read_first(&mut self, reader: &mut BitReader<'_>) -> Result<(), ChunkError> {
        self.timestamp = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        for (value, _) in self
            .scalars
            .iter_mut()
            .chain(self.positive.iter_mut())
            .chain(self.negative.iter_mut())
        {
            *value = f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?);
        }
        Ok(())
    }

    fn read_next(&mut self, reader: &mut BitReader<'_>) -> Result<bool, ChunkError> {
        let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        self.timestamp_delta = self.timestamp_delta.wrapping_add(dod);
        self.timestamp = self.timestamp.wrapping_add(self.timestamp_delta);
        for (value, state) in &mut self.scalars {
            reader
                .read_xor_value(value, state)
                .ok_or(ChunkError::Truncated)?;
        }
        if self.scalars[2].0.to_bits() == STALE_NAN_BITS {
            return Ok(true);
        }
        for (value, state) in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            reader
                .read_xor_value(value, state)
                .ok_or(ChunkError::Truncated)?;
        }
        Ok(false)
    }

    fn histogram(
        &self,
        layout: &HistogramLayout,
        reset_hint: ResetHint,
    ) -> Result<NativeHistogram, ChunkError> {
        Ok(NativeHistogram {
            schema: layout.schema,
            is_float: true,
            reset_hint,
            zero_threshold: layout.zero_threshold,
            zero_count: self.scalars[1].0,
            count: self.scalars[0].0,
            sum: self.scalars[2].0,
            positive_spans: layout.positive_spans.clone(),
            positive_counts: self.positive.iter().map(|(value, _)| *value).collect(),
            negative_spans: layout.negative_spans.clone(),
            negative_counts: self.negative.iter().map(|(value, _)| *value).collect(),
            custom_values: layout.custom_values.clone(),
            start_timestamp_ms: None,
        })
    }
}
