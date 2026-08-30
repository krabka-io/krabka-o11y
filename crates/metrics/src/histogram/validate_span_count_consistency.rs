use super::{BucketSpan, HistogramCodecError, span_bucket_total};

pub(crate) fn validate_span_count_consistency(
    spans: &[BucketSpan],
    counts: &[f64],
) -> Result<(), HistogramCodecError> {
    let span_total = span_bucket_total(spans);
    if span_total == counts.len() {
        Ok(())
    } else {
        Err(HistogramCodecError::SpanCountMismatch {
            spans: span_total,
            counts: counts.len(),
        })
    }
}
