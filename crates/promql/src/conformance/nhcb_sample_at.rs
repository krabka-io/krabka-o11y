use super::{
    Line, NhcbBucketSeries, Result, SampleSpec, cumulative_to_bucket_counts,
    native_custom_bucket_histogram, parse_error,
};

pub(crate) fn nhcb_sample_at(
    buckets: &[NhcbBucketSeries],
    sum_values: Option<&[SampleSpec]>,
    custom_values: &[f64],
    index: usize,
    line: Line<'_>,
) -> Result<SampleSpec> {
    let mut cumulative = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        match bucket.values.get(index) {
            Some(SampleSpec::Value(value)) => cumulative.push(*value),
            Some(SampleSpec::Missing) | None => return Ok(SampleSpec::Missing),
            Some(SampleSpec::Stale) => return Ok(SampleSpec::Stale),
            Some(SampleSpec::Histogram(_) | SampleSpec::String(_)) => {
                return Err(parse_error(
                    line,
                    "load_with_nhcb bucket samples must be float values",
                ));
            }
        }
    }
    let counts = cumulative_to_bucket_counts(&cumulative);
    let total = cumulative.last().copied().unwrap_or(0.0);
    let sum = match sum_values.and_then(|values| values.get(index)) {
        Some(SampleSpec::Value(value)) => *value,
        Some(SampleSpec::Missing) => return Ok(SampleSpec::Missing),
        Some(SampleSpec::Stale) => return Ok(SampleSpec::Stale),
        Some(SampleSpec::Histogram(_) | SampleSpec::String(_)) => {
            return Err(parse_error(
                line,
                "load_with_nhcb sum samples must be float values",
            ));
        }
        None => 0.0,
    };
    Ok(SampleSpec::Histogram(native_custom_bucket_histogram(
        custom_values,
        &counts,
        total,
        sum,
        line,
    )?))
}
