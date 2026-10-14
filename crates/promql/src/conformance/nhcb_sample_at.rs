use super::{
    Line, NhcbBucketSeries, Result, SampleSpec, cumulative_to_bucket_counts,
    native_custom_bucket_histogram, parse_error,
};

enum FloatSampleAt {
    Value(f64),
    Missing,
    Stale,
}

fn float_sample_at(
    sample: Option<&SampleSpec>,
    line: Line<'_>,
    role: &str,
) -> Result<FloatSampleAt> {
    match sample {
        Some(SampleSpec::Value(value)) => Ok(FloatSampleAt::Value(*value)),
        Some(SampleSpec::Missing) | None => Ok(FloatSampleAt::Missing),
        Some(SampleSpec::Stale) => Ok(FloatSampleAt::Stale),
        Some(SampleSpec::Histogram(_) | SampleSpec::String(_)) => Err(parse_error(
            line,
            format!("load_with_nhcb {role} samples must be float values"),
        )),
    }
}

pub(crate) fn nhcb_sample_at(
    buckets: &[NhcbBucketSeries],
    sum_values: Option<&[SampleSpec]>,
    custom_values: &[f64],
    index: usize,
    line: Line<'_>,
) -> Result<SampleSpec> {
    let mut cumulative = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        match float_sample_at(bucket.values.get(index), line, "bucket")? {
            FloatSampleAt::Value(value) => cumulative.push(value),
            FloatSampleAt::Missing => return Ok(SampleSpec::Missing),
            FloatSampleAt::Stale => return Ok(SampleSpec::Stale),
        }
    }
    let counts = cumulative_to_bucket_counts(&cumulative);
    let total = cumulative.last().copied().unwrap_or(0.0);
    let sum_sample = sum_values.and_then(|values| values.get(index));
    let sum = match float_sample_at(sum_sample, line, "sum")? {
        FloatSampleAt::Value(value) => value,
        FloatSampleAt::Missing if sum_sample.is_none() => 0.0,
        FloatSampleAt::Missing => return Ok(SampleSpec::Missing),
        FloatSampleAt::Stale => return Ok(SampleSpec::Stale),
    };
    Ok(SampleSpec::Histogram(native_custom_bucket_histogram(
        custom_values,
        &counts,
        total,
        sum,
        line,
    )?))
}
