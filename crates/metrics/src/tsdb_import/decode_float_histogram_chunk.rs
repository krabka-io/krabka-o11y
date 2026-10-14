use super::{
    BitReader, ChunkError, HistogramChunkState, HistogramLayout, NativeHistogram, ResetHint,
    STALE_NAN_BITS, XorState, decode_histogram_samples,
    histogram_chunk_state::{read_initial_timestamp, read_next_timestamp},
    stale_histogram,
};

/// Decodes a Prometheus float native-histogram chunk.
///
/// The first sample stores every value as 64 raw bits. Each later sample
/// stores the count, the zero count, the sum and every bucket as a Gorilla XOR
/// against the same value in the sample before, each with its own state.
pub fn decode_float_histogram_chunk(
    chunk: &[u8],
    max_buckets: u64,
) -> Result<Vec<super::TimestampedHistogramSample>, ChunkError> {
    decode_histogram_samples::<FloatState>(chunk, max_buckets)
}

struct FloatState {
    timestamp: i64,
    timestamp_delta: i64,
    /// The count, zero count and sum, in that order.
    scalars: [FloatXorValue; 3],
    positive: Vec<FloatXorValue>,
    negative: Vec<FloatXorValue>,
}

#[derive(Clone, Copy)]
struct FloatXorValue {
    value: f64,
    state: XorState,
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
            scalars: [FloatXorValue::default(); 3],
            positive: vec![FloatXorValue::default(); layout.positive_buckets],
            negative: vec![FloatXorValue::default(); layout.negative_buckets],
        }
    }

    fn read_first(&mut self, reader: &mut BitReader<'_>) -> Result<(), ChunkError> {
        self.timestamp = read_initial_timestamp(reader)?;
        for value in self
            .scalars
            .iter_mut()
            .chain(self.positive.iter_mut())
            .chain(self.negative.iter_mut())
        {
            value.value = f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?);
        }
        Ok(())
    }

    fn read_next(&mut self, reader: &mut BitReader<'_>) -> Result<bool, ChunkError> {
        read_next_timestamp(reader, &mut self.timestamp_delta, &mut self.timestamp)?;
        for decoded in &mut self.scalars {
            reader
                .read_xor_value(&mut decoded.value, &mut decoded.state)
                .ok_or(ChunkError::Truncated)?;
        }
        if self.scalars[2].value.to_bits() == STALE_NAN_BITS {
            return Ok(true);
        }
        for decoded in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            reader
                .read_xor_value(&mut decoded.value, &mut decoded.state)
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
            zero_count: self.scalars[1].value,
            count: self.scalars[0].value,
            sum: self.scalars[2].value,
            positive_spans: layout.positive_spans.clone(),
            positive_counts: self.positive.iter().map(|decoded| decoded.value).collect(),
            negative_spans: layout.negative_spans.clone(),
            negative_counts: self.negative.iter().map(|decoded| decoded.value).collect(),
            custom_values: layout.custom_values.clone(),
            start_timestamp_ms: None,
        })
    }
}

impl Default for FloatXorValue {
    fn default() -> Self {
        Self {
            value: 0.0,
            state: XorState::default(),
        }
    }
}
