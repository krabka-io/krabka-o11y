use super::{
    BitReader, ChunkError, HistogramLayout, NativeHistogram, STALE_NAN_BITS, XorState,
    counter_reset_hint, i64_to_f64, stale_histogram, u64_to_f64,
};

/// Decodes a Prometheus integer native-histogram chunk.
///
/// The chunk stores bucket counts as deltas between neighbouring buckets, and
/// each sample after the first as a delta of those deltas. The decoder turns
/// both into the absolute per-bucket counts that Krabka stores.
pub fn decode_histogram_chunk(
    data: &[u8],
    max_buckets: u64,
) -> Result<Vec<(i64, NativeHistogram)>, ChunkError> {
    let (header, body) = data.split_at_checked(3).ok_or(ChunkError::Truncated)?;
    let total = u16::from_be_bytes([header[0], header[1]]);
    let reset_header = header[2];
    if total == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(body);
    let layout = HistogramLayout::read(&mut reader, max_buckets)?;
    let mut state = IntegerState::new(&layout);
    let mut samples = Vec::with_capacity(usize::from(total));
    for index in 0..total {
        let is_stale = if index == 0 {
            state.read_first(&mut reader)?;
            false
        } else {
            state.read_next(&mut reader)?
        };
        let histogram = if is_stale {
            stale_histogram(false)
        } else {
            state.histogram(&layout, counter_reset_hint(reset_header, index))?
        };
        samples.push((state.timestamp, histogram));
    }
    Ok(samples)
}

struct IntegerState {
    timestamp: i64,
    timestamp_delta: i64,
    count: u64,
    count_delta: i64,
    zero_count: u64,
    zero_count_delta: i64,
    sum: f64,
    sum_state: XorState,
    positive: Vec<i64>,
    positive_delta: Vec<i64>,
    negative: Vec<i64>,
    negative_delta: Vec<i64>,
}

impl IntegerState {
    fn new(layout: &HistogramLayout) -> Self {
        Self {
            timestamp: 0,
            timestamp_delta: 0,
            count: 0,
            count_delta: 0,
            zero_count: 0,
            zero_count_delta: 0,
            sum: 0.0,
            sum_state: XorState::default(),
            positive: vec![0; layout.positive_buckets],
            positive_delta: vec![0; layout.positive_buckets],
            negative: vec![0; layout.negative_buckets],
            negative_delta: vec![0; layout.negative_buckets],
        }
    }

    fn read_first(&mut self, reader: &mut BitReader<'_>) -> Result<(), ChunkError> {
        self.timestamp = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        self.count = reader.read_varbit_uint().ok_or(ChunkError::Truncated)?;
        self.zero_count = reader.read_varbit_uint().ok_or(ChunkError::Truncated)?;
        self.sum = f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?);
        for bucket in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            *bucket = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        }
        Ok(())
    }

    /// Reads one later sample and reports whether it is a stale marker, which
    /// carries no buckets.
    fn read_next(&mut self, reader: &mut BitReader<'_>) -> Result<bool, ChunkError> {
        let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        self.timestamp_delta = self.timestamp_delta.wrapping_add(dod);
        self.timestamp = self.timestamp.wrapping_add(self.timestamp_delta);
        let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        self.count_delta = self.count_delta.wrapping_add(dod);
        self.count = self
            .count
            .cast_signed()
            .wrapping_add(self.count_delta)
            .cast_unsigned();
        let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        self.zero_count_delta = self.zero_count_delta.wrapping_add(dod);
        self.zero_count = self
            .zero_count
            .cast_signed()
            .wrapping_add(self.zero_count_delta)
            .cast_unsigned();
        reader
            .read_xor_value(&mut self.sum, &mut self.sum_state)
            .ok_or(ChunkError::Truncated)?;
        if self.sum.to_bits() == STALE_NAN_BITS {
            return Ok(true);
        }
        for (bucket, delta) in self
            .positive
            .iter_mut()
            .zip(self.positive_delta.iter_mut())
            .chain(self.negative.iter_mut().zip(self.negative_delta.iter_mut()))
        {
            let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
            *delta = delta.wrapping_add(dod);
            *bucket = bucket.wrapping_add(*delta);
        }
        Ok(false)
    }

    fn histogram(
        &self,
        layout: &HistogramLayout,
        reset_hint: super::ResetHint,
    ) -> Result<NativeHistogram, ChunkError> {
        Ok(NativeHistogram {
            schema: layout.schema,
            is_float: false,
            reset_hint,
            zero_threshold: layout.zero_threshold,
            zero_count: u64_to_f64(self.zero_count),
            count: u64_to_f64(self.count),
            sum: self.sum,
            positive_spans: layout.positive_spans.clone(),
            positive_counts: absolute_counts(&self.positive)?,
            negative_spans: layout.negative_spans.clone(),
            negative_counts: absolute_counts(&self.negative)?,
            custom_values: layout.custom_values.clone(),
            start_timestamp_ms: None,
        })
    }
}

/// Turns the bucket-to-bucket deltas into absolute counts. A running total
/// below zero is not a count, so the chunk is refused.
fn absolute_counts(deltas: &[i64]) -> Result<Vec<f64>, ChunkError> {
    let mut current = 0_i64;
    deltas
        .iter()
        .map(|delta| {
            current = current.wrapping_add(*delta);
            if current < 0 {
                return Err(ChunkError::Invalid(format!(
                    "histogram bucket count {current} is negative"
                )));
            }
            Ok(i64_to_f64(current))
        })
        .collect()
}
