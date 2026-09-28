use super::{BitReader, BucketSpan, ChunkError, u64_to_f64};

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
