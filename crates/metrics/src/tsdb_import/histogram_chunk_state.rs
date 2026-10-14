use super::{
    BitReader, ChunkError, HistogramLayout, NativeHistogram, ResetHint, TimestampedHistogramSample,
    counter_reset_hint,
};

/// The per-sample decoding state of one native-histogram chunk encoding.
///
/// The integer and float encodings share the chunk header, the layout and the
/// walk over samples; they differ in how a sample's values are stored, which
/// is what an implementor reads.
pub trait HistogramChunkState: Sized {
    /// The stale marker of this encoding.
    fn stale_marker() -> NativeHistogram;

    fn new(layout: &HistogramLayout) -> Self;

    fn read_first(&mut self, reader: &mut BitReader<'_>) -> Result<(), ChunkError>;

    /// Reads the next sample and returns whether it is a stale marker.
    fn read_next(&mut self, reader: &mut BitReader<'_>) -> Result<bool, ChunkError>;

    fn histogram(
        &self,
        layout: &HistogramLayout,
        reset_hint: ResetHint,
    ) -> Result<NativeHistogram, ChunkError>;

    fn timestamp(&self) -> i64;
}

pub(super) fn read_initial_timestamp(reader: &mut BitReader<'_>) -> Result<i64, ChunkError> {
    reader.read_varbit_int().ok_or(ChunkError::Truncated)
}

pub(super) fn read_next_timestamp(
    reader: &mut BitReader<'_>,
    timestamp_delta: &mut i64,
    timestamp: &mut i64,
) -> Result<(), ChunkError> {
    let delta_of_delta = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
    *timestamp_delta = timestamp_delta.wrapping_add(delta_of_delta);
    *timestamp = timestamp.wrapping_add(*timestamp_delta);
    Ok(())
}

/// Decodes the samples of a native-histogram chunk in the encoding `S` reads.
pub fn decode_histogram_samples<S: HistogramChunkState>(
    chunk: &[u8],
    max_buckets: u64,
) -> Result<Vec<TimestampedHistogramSample>, ChunkError> {
    let parts = chunk.split_at_checked(3).ok_or(ChunkError::Truncated)?;
    let header = parts.0;
    let body = parts.1;
    let total = u16::from_be_bytes([header[0], header[1]]);
    let reset_header = header[2];
    if total == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(body);
    let layout = HistogramLayout::read(&mut reader, max_buckets)?;
    let mut state = S::new(&layout);
    let mut samples = Vec::with_capacity(usize::from(total));
    for index in 0..total {
        let is_stale = if index == 0 {
            state.read_first(&mut reader)?;
            false
        } else {
            state.read_next(&mut reader)?
        };
        let histogram = if is_stale {
            S::stale_marker()
        } else {
            state.histogram(&layout, counter_reset_hint(reset_header, index))?
        };
        samples.push(TimestampedHistogramSample {
            timestamp_ms: state.timestamp(),
            histogram,
        });
    }
    Ok(samples)
}
