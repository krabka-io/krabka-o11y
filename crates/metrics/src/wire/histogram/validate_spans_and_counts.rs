use super::*;

/// Strict span and count validation that matches the Prometheus appender. It
/// runs at the wire edge before the module admits a histogram.
///
/// For both the positive and the negative buckets, the sum of the span lengths
/// must equal the number of decoded counts. Prometheus does the same in
/// `Histogram.Validate` and `FloatHistogram.Validate`. For NHCB, which is
/// schema `-53` with custom buckets, the histogram must carry no negative
/// buckets, and `custom_values` must define an upper bound for every populated
/// positive bucket.
pub(crate) fn validate_spans_and_counts(
    schema: i8,
    positive_spans: &[BucketSpan],
    positive_counts: &[f64],
    negative_spans: &[BucketSpan],
    negative_counts: &[f64],
    custom_values: Option<&[f64]>,
) -> Result<(), WireError> {
    check_side("positive", positive_spans, positive_counts.len())?;
    check_side("negative", negative_spans, negative_counts.len())?;

    if schema == -53 {
        // NHCB: custom buckets are exclusively positive; the boundaries in
        // `custom_values` must cover every populated positive bucket.
        if !negative_spans.is_empty() || !negative_counts.is_empty() {
            return Err(WireError::Invalid(
                "custom-bucket histogram must not carry negative buckets".to_string(),
            ));
        }
        let buckets = span_bucket_total(positive_spans);
        let bounds = custom_values.map_or(0, <[f64]>::len);
        if buckets > bounds {
            return Err(WireError::Invalid(format!(
                "custom-bucket histogram has {buckets} populated buckets but only {bounds} custom values"
            )));
        }
    }

    Ok(())
}
