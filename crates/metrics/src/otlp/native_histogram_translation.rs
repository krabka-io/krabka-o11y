use super::*;
use crate::SpannedHistogramCounts;


/// Converts one OTLP exponential histogram point to a native histogram
/// sample.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn exponential_histogram_to_native(
    point: &ExponentialHistogramDataPoint,
) -> Result<NativeHistogram, OtlpError> {
    if point.scale < MIN_NATIVE_HISTOGRAM_SCHEMA {
        return Err(OtlpError::Invalid(
            "exponential histogram".into(),
            format!(
                "scale {} is below native histogram minimum schema -4",
                point.scale
            ),
        ));
    }
    let schema = point.scale.min(MAX_NATIVE_HISTOGRAM_SCHEMA);
    let positive = downscaled_spans(point.positive.as_ref(), point.scale, schema)?;
    let negative = downscaled_spans(point.negative.as_ref(), point.scale, schema)?;

    Ok(NativeHistogram {
        schema: i8::try_from(schema).map_err(|_| {
            OtlpError::Invalid(
                "exponential histogram".into(),
                format!("scale {schema} out of range"),
            )
        })?,
        is_float: false,
        reset_hint: ResetHint::Unknown,
        zero_threshold: point.zero_threshold,
        zero_count: point.zero_count.to_f64().unwrap_or(f64::MAX),
        count: point.count.to_f64().unwrap_or(f64::MAX),
        sum: point.sum.unwrap_or(0.0),
        positive_spans: positive.spans,
        positive_counts: positive.counts,
        negative_spans: negative.spans,
        negative_counts: negative.counts,
        custom_values: None,
        start_timestamp_ms: (point.start_time_unix_nano != 0)
            .then_some(nanos_to_millis(point.start_time_unix_nano)),
    })
}

pub(crate) fn downscaled_spans(
    buckets: Option<&exponential_histogram_data_point::Buckets>,
    source_schema: i32,
    target_schema: i32,
) -> Result<SpannedHistogramCounts, OtlpError> {
    let Some(buckets) = buckets else {
        return Ok(SpannedHistogramCounts::default());
    };
    let shift = u32::try_from(source_schema - target_schema)
        .map_err(|_| OtlpError::Invalid("exponential histogram".into(), "invalid scale".into()))?;
    let divisor = 1_i32.checked_shl(shift).ok_or_else(|| {
        OtlpError::Invalid(
            "exponential histogram".into(),
            format!("scale {source_schema} cannot be downscaled to schema {target_schema}"),
        )
    })?;
    let mut merged = BTreeMap::<i32, u64>::new();
    for bucket in buckets.bucket_counts.iter().enumerate() {
        let idx = i32::try_from(bucket.0).map_err(|_| {
            OtlpError::Invalid("exponential histogram".into(), "too many buckets".into())
        })?;
        let original_offset = buckets.offset.checked_add(idx).ok_or_else(|| {
            OtlpError::Invalid(
                "exponential histogram".into(),
                "bucket offset overflow".into(),
            )
        })?;
        let offset = original_offset
            .div_euclid(divisor)
            .checked_add(1)
            .ok_or_else(|| {
                OtlpError::Invalid(
                    "exponential histogram".into(),
                    "bucket offset overflow".into(),
                )
            })?;
        let merged_count = merged.entry(offset).or_default();
        if target_schema < source_schema && *merged_count != 0 && *bucket.1 != 0 {
            return Err(OtlpError::Invalid(
                "exponential histogram".into(),
                format!(
                    "scale {source_schema} cannot be downscaled to schema {target_schema} without lossy downscale"
                ),
            ));
        }
        *merged_count += bucket.1;
    }

    // Counts are carried as f64 downstream, and the span encoder is shared
    // with the merge path so both produce the same delta-offset form.
    let mut float_counts = BTreeMap::new();
    for bucket in merged {
        float_counts.insert(bucket.0, bucket.1.to_f64().unwrap_or(f64::MAX));
    }
    Ok(compact_spanned_histogram_counts(float_counts))
}

pub(crate) fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    // The first span's offset is absolute and every later one is a delta from
    // where the previous span ended. Starting the running index at zero makes
    // those the same operation.
    for span in spans {
        index += span.offset;
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}

pub(crate) fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> SpannedHistogramCounts {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for bucket in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(bucket.0).or_insert(0.0) += bucket.1;
    }
    compact_spanned_histogram_counts(buckets)
}

pub(crate) fn add_compatible_native_histogram(
    metric_name: &str,
    cumulative: &mut NativeHistogram,
    delta: &NativeHistogram,
) -> Result<(), OtlpError> {
    if cumulative.schema != delta.schema
        || cumulative.is_float != delta.is_float
        || cumulative.reset_hint != delta.reset_hint
        || cumulative.zero_threshold.to_bits() != delta.zero_threshold.to_bits()
        || cumulative.custom_values != delta.custom_values
    {
        return Err(OtlpError::Invalid(
            metric_name.into(),
            "incompatible delta exponential histogram layout".into(),
        ));
    }

    cumulative.zero_count += delta.zero_count;
    cumulative.count += delta.count;
    cumulative.sum += delta.sum;
    let positive = add_spanned_histogram_counts(
        &cumulative.positive_spans,
        &cumulative.positive_counts,
        &delta.positive_spans,
        &delta.positive_counts,
    );
    let negative = add_spanned_histogram_counts(
        &cumulative.negative_spans,
        &cumulative.negative_counts,
        &delta.negative_spans,
        &delta.negative_counts,
    );
    cumulative.positive_spans = positive.spans;
    cumulative.positive_counts = positive.counts;
    cumulative.negative_spans = negative.spans;
    cumulative.negative_counts = negative.counts;
    Ok(())
}
