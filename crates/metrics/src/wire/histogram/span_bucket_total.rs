use super::BucketSpan;

/// The number of buckets a run of spans declares.
///
/// The lengths come off the wire unchecked, so the sum saturates rather than
/// wrapping: a total that large disagrees with any decoded count vector, and
/// `check_side` turns the disagreement into a decode error.
pub(crate) fn span_bucket_total(spans: &[BucketSpan]) -> usize {
    spans.iter().fold(0_usize, |total, span| {
        total.saturating_add(span.length as usize)
    })
}
