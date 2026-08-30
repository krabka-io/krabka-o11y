use super::{
    ExponentialHistogramDataPoint, MAX_NATIVE_HISTOGRAM_SCHEMA, MIN_NATIVE_HISTOGRAM_SCHEMA,
    NativeHistogram, OtlpError, ResetHint, ToPrimitive, downscaled_spans, nanos_to_millis,
};

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
    let (positive_spans, positive_counts) =
        downscaled_spans(point.positive.as_ref(), point.scale, schema)?;
    let (negative_spans, negative_counts) =
        downscaled_spans(point.negative.as_ref(), point.scale, schema)?;

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
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values: None,
        start_timestamp_ms: (point.start_time_unix_nano != 0)
            .then_some(nanos_to_millis(point.start_time_unix_nano)),
    })
}
