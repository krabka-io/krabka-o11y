use super::{ToPrimitive, WireError};

/// The absolute bucket counts for one side of a histogram.
///
/// A float histogram states them outright. An integer histogram states them as
/// a run of deltas whose running total is an `i64`, and a run that leaves that
/// range is malformed input rather than a count: the sample is refused.
///
/// Prometheus arrives at the same verdict from the other side. It adds the
/// deltas in a wrapping `int64` and then refuses any bucket whose absolute
/// count came out negative, in `checkHistogramBuckets`, so an overflowing run
/// of positive deltas is rejected there too. Saturating instead would hand the
/// query path a bucket count that nothing downstream could tell from a real
/// one, which is the worse answer for input that arrives unauthenticated.
pub(crate) fn counts(float_counts: &[f64], deltas: &[i64]) -> Result<Vec<f64>, WireError> {
    if !float_counts.is_empty() {
        return Ok(float_counts.to_vec());
    }

    let mut total = 0_i64;
    deltas
        .iter()
        .enumerate()
        .map(|(bucket, delta)| {
            let next = total.checked_add(*delta).ok_or_else(|| {
                WireError::Invalid(format!(
                    "histogram bucket delta run overflows i64 at bucket {bucket}: \
                     count {total} plus delta {delta}"
                ))
            })?;
            total = next;
            Ok(total.to_f64().unwrap_or(f64::MAX))
        })
        .collect()
}
