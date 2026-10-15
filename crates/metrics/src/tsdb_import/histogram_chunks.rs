use super::{
    BitReader, BucketSpan, ChunkError, NativeHistogram, ResetHint, STALE_NAN_BITS,
    TimestampedHistogramSample, XorState, i64_to_f64, stale_histogram, u64_to_f64,
};

/// The counter-reset hint of the sample at `index` in a histogram chunk.
///
/// This matches Prometheus `counterResetHint`: a gauge chunk marks every
/// sample as a gauge, and a counter chunk marks every sample after the first
/// as not a reset. The first sample is unknown, because the chunk before it
/// may be gone or may have been written later by a backfill.
pub fn counter_reset_hint(header: u8, index: u16) -> ResetHint {
    const GAUGE: u8 = 0xc0;
    if header & GAUGE == GAUGE {
        ResetHint::Gauge
    } else if index > 0 {
        ResetHint::No
    } else {
        ResetHint::Unknown
    }
}

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

/// The part of a histogram chunk header that every sample in the chunk
/// shares: the schema, the zero threshold, the bucket spans and, for custom
/// buckets, the bucket bounds.
#[derive(Clone, Debug, PartialEq)]
pub struct HistogramLayout {
    pub schema: i8,
    pub zero_threshold: f64,
    pub positive_spans: Vec<BucketSpan>,
    pub negative_spans: Vec<BucketSpan>,
    pub custom_values: Option<Vec<f64>>,
    pub positive_buckets: usize,
    pub negative_buckets: usize,
}

/// The schema of a native histogram with custom bucket bounds (NHCB).
const CUSTOM_BUCKETS_SCHEMA: i64 = -53;

impl HistogramLayout {
    /// Reads the layout and checks it against `max_buckets`, the bound for any
    /// one span list, bucket count or bound list.
    pub fn read(reader: &mut BitReader<'_>, max_buckets: u64) -> Result<Self, ChunkError> {
        let zero_threshold = match reader.read_byte().ok_or(ChunkError::Truncated)? {
            0 => 0.0,
            255 => f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?),
            exponent => ldexp_half(i32::from(exponent) - 243),
        };
        let raw_schema = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        let schema = if raw_schema == CUSTOM_BUCKETS_SCHEMA || (-4..=8).contains(&raw_schema) {
            i8::try_from(raw_schema).map_err(|_| ChunkError::Schema(raw_schema))?
        } else {
            return Err(ChunkError::Schema(raw_schema));
        };
        let positive_spans = read_spans(reader, max_buckets)?;
        let negative_spans = read_spans(reader, max_buckets)?;
        let custom_values = if raw_schema == CUSTOM_BUCKETS_SCHEMA {
            Some(read_custom_bounds(reader, max_buckets)?)
        } else {
            None
        };
        let positive_buckets = bucket_count(&positive_spans, max_buckets)?;
        let negative_buckets = bucket_count(&negative_spans, max_buckets)?;
        Ok(Self {
            schema,
            zero_threshold,
            positive_spans,
            negative_spans,
            custom_values,
            positive_buckets,
            negative_buckets,
        })
    }
}

/// `0.5 * 2^exponent`, the zero threshold that one header byte encodes.
fn ldexp_half(exponent: i32) -> f64 {
    // The byte range 1..=254 gives exponents -242..=11, and every one of them
    // is a normal `f64`, so the biased exponent field holds it exactly.
    let biased = u64::try_from(exponent - 1 + 1023).unwrap_or(0);
    f64::from_bits(biased << 52)
}

fn read_spans(reader: &mut BitReader<'_>, max_buckets: u64) -> Result<Vec<BucketSpan>, ChunkError> {
    let count = reader.read_varbit_uint().ok_or(ChunkError::Truncated)?;
    let count = bounded(count, max_buckets)?;
    let mut spans = Vec::with_capacity(count);
    for _ in 0..count {
        let length = reader.read_varbit_uint().ok_or(ChunkError::Truncated)?;
        let offset = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
        spans.push(BucketSpan {
            offset: i32::try_from(offset)
                .map_err(|_| ChunkError::Invalid(format!("span offset {offset} overflows i32")))?,
            length: u32::try_from(length)
                .map_err(|_| ChunkError::Invalid(format!("span length {length} overflows u32")))?,
        });
    }
    Ok(spans)
}

fn read_custom_bounds(
    reader: &mut BitReader<'_>,
    max_buckets: u64,
) -> Result<Vec<f64>, ChunkError> {
    let count = reader.read_varbit_uint().ok_or(ChunkError::Truncated)?;
    let count = bounded(count, max_buckets)?;
    let mut bounds = Vec::with_capacity(count);
    for _ in 0..count {
        let bound = match reader.read_varbit_uint().ok_or(ChunkError::Truncated)? {
            0 => f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?),
            // Prometheus stores a bound with three decimal places as
            // `bound * 1000 + 1`. The value fits in 56 bits, so the `f64`
            // division reproduces the Go `float64(b-1) / 1000` exactly.
            thousandths => u64_to_f64(thousandths - 1) / 1000.0,
        };
        bounds.push(bound);
    }
    Ok(bounds)
}

fn bucket_count(spans: &[BucketSpan], max_buckets: u64) -> Result<usize, ChunkError> {
    let total = spans.iter().fold(0_u64, |total, span| {
        total.saturating_add(u64::from(span.length))
    });
    bounded(total, max_buckets)
}

fn bounded(count: u64, max_buckets: u64) -> Result<usize, ChunkError> {
    if count > max_buckets {
        return Err(ChunkError::TooManyBuckets(count));
    }
    usize::try_from(count).map_err(|_| ChunkError::TooManyBuckets(count))
}

/// Decodes a Prometheus integer native-histogram chunk.
///
/// The chunk stores bucket counts as deltas between neighbouring buckets, and
/// each sample after the first as a delta of those deltas. The decoder turns
/// both into the absolute per-bucket counts that Krabka stores.
pub fn decode_histogram_chunk(
    chunk: &[u8],
    max_buckets: u64,
) -> Result<Vec<super::TimestampedHistogramSample>, ChunkError> {
    decode_histogram_samples::<IntegerState>(chunk, max_buckets)
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

impl HistogramChunkState for IntegerState {
    fn stale_marker() -> NativeHistogram {
        stale_histogram(false)
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

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
        self.timestamp = read_initial_timestamp(reader)?;
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
        read_next_timestamp(reader, &mut self.timestamp_delta, &mut self.timestamp)?;
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
        for bucket_delta in self
            .positive
            .iter_mut()
            .zip(self.positive_delta.iter_mut())
            .chain(self.negative.iter_mut().zip(self.negative_delta.iter_mut()))
        {
            let dod = reader.read_varbit_int().ok_or(ChunkError::Truncated)?;
            *bucket_delta.1 = bucket_delta.1.wrapping_add(dod);
            *bucket_delta.0 = bucket_delta.0.wrapping_add(*bucket_delta.1);
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
