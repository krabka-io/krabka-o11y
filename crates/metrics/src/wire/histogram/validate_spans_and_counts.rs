use super::{BucketSpan, WireError, check_side, span_bucket_total};

/// Strict span and count validation that matches the Prometheus appender. It
/// runs at the wire edge before the module admits a histogram.
///
/// For both the positive and the negative buckets, the sum of the span lengths
/// must equal the number of decoded counts. Prometheus does the same in
/// `Histogram.Validate` and `FloatHistogram.Validate`. For NHCB, which is
/// schema `-53` with custom buckets, the histogram must carry no negative
/// buckets, and `custom_values` must define an upper bound for every populated
/// positive bucket bar the last.
///
/// That last bucket is the reason the bound count is one short of the bucket
/// count rather than equal to it: a custom-bucket histogram's final bucket is
/// the implicit `+Inf` one, which needs no bound. Prometheus refuses only
/// `totalSpanLength > len(bounds) + 1`, and a classic histogram converted to
/// NHCB -- every `load_with_nhcb` block of the vendored corpus -- lands exactly
/// on that boundary. A classic histogram whose only bucket is `+Inf` converts
/// to one bucket and no bounds at all, and the pinned Prometheus image accepts
/// that too, so no bound count is a floor of its own.
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
        // `custom_values` must cover every populated positive bucket but the
        // implicit `+Inf` one.
        if !negative_spans.is_empty() || !negative_counts.is_empty() {
            return Err(WireError::Invalid(
                "custom-bucket histogram must not carry negative buckets".to_string(),
            ));
        }
        let buckets = span_bucket_total(positive_spans);
        let bounds = custom_values.map_or(0, <[f64]>::len);
        // Prometheus counts the span offsets too: a gap between spans is a run
        // of empty buckets that still needs bounds behind it.
        let spanned = buckets.saturating_add(span_offset_total(positive_spans));
        if spanned > bounds + 1 {
            return Err(WireError::Invalid(format!(
                "custom-bucket histogram spans {spanned} buckets but only {bounds} custom values \
                 define bounds for them"
            )));
        }
    }

    Ok(())
}

/// The buckets the span offsets skip over, which Prometheus counts towards the
/// span length a histogram's bounds have to cover.
fn span_offset_total(spans: &[BucketSpan]) -> usize {
    spans.iter().fold(0_usize, |total, span| {
        total.saturating_add(usize::try_from(span.offset).unwrap_or(0))
    })
}
