use std::num::FpCategory;

use super::{
    ClassicBucket, HistogramReducers, InstantSample, NativeHistogram, NativeQuantileBucket,
    NormalizedClassicBuckets, Result, apply_histogram_reduction,
    emit_histogram_quantile_forced_monotonicity_info, emit_info, emit_warning,
    invalid_quantile_warning, is_valid_quantile, native_histogram_all_buckets,
    native_histogram_bucket_quantile, native_histogram_quantile_nan_result_info,
    native_histogram_quantile_nan_skew_info, normalized_classic_histogram_buckets,
    zero_bucket_bounds,
};

/// Prometheus. Both the `__name__` and `le` labels are dropped from every output
/// series. Classic output samples carry `time_ms`; native ones keep the source
/// sample timestamp.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_quantile(
    quantile: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    // `funcHistogramQuantile` warns about an out-of-range quantile before it
    // looks at a single sample, so an empty input vector still warns.
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    apply_histogram_reduction(
        samples,
        time_ms,
        HistogramReducers {
            native: |histogram: &NativeHistogram, metric: &str| {
                native_histogram_quantile(quantile, histogram, metric)
            },
            classic: |metric: &str, buckets: &mut Vec<ClassicBucket>| {
                let result = classic_histogram_quantile(quantile, buckets);
                if result.forced {
                    emit_histogram_quantile_forced_monotonicity_info(
                        metric,
                        time_ms,
                        result.repairs,
                    );
                }
                result.value
            },
        },
    )
}

#[cfg(feature = "experimental-functions")]
use krabka_metrics::NativeHistogram;

#[cfg(feature = "experimental-functions")]
use super::{
    BTreeMap, ClassicBucketGroups, InstantSample, Result, SampleValue, classic_histogram_quantile,
    emit_histogram_quantile_forced_monotonicity_info, emit_warning, find_mixed_histogram_keys,
    format_quantile_label, group_classic_bucket_sample, invalid_quantile_warning,
    is_valid_quantile, labels_key, labels_without_metric_name, native_histogram_quantile,
    record_metric_name,
};

#[cfg(feature = "experimental-functions")]
struct NativeHistogramSample {
    labels: super::Labels,
    timestamp_ms: i64,
    histogram: NativeHistogram,
}

/// Applies the experimental `histogram_quantiles(label, v, phi...)` fold.
///
/// The input is an already-evaluated instant vector. This function emits one
/// output series for each `(input series, quantile)` pair and writes the
/// quantile into the label that `label` names.
///
/// The interpreter method `PromqlEngine::eval_histogram_quantiles_call` and the
/// operator-path `histogram_quantiles` dispatch share this function, so the two
/// match Prometheus by construction. This holds for classic
/// `<metric>_bucket{le}` float-bucket vectors and for native-histogram vectors.
/// Mixed classic and native groups emit the same warning as `histogram_quantile`.
/// Classic output samples carry `time_ms`, and
/// native output samples keep the source sample timestamp. Both drop `__name__`,
/// and classic buckets also drop `le`.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
#[cfg(feature = "experimental-functions")]
pub(crate) fn apply_histogram_quantiles(
    samples: Vec<InstantSample>,
    label_name: &str,
    quantiles: &[f64],
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    for quantile in quantiles {
        if !is_valid_quantile(*quantile) {
            emit_warning(invalid_quantile_warning(*quantile));
        }
    }
    let mut groups = ClassicBucketGroups::new();
    let mut native_samples = BTreeMap::new();
    let mut metric_names = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            record_metric_name(&mut metric_names, &labels_key(&labels), &sample.labels);
            native_samples.insert(
                labels_key(&labels),
                NativeHistogramSample {
                    labels,
                    timestamp_ms: sample.ts_ms,
                    histogram: histogram.clone(),
                },
            );
            continue;
        }
        group_classic_bucket_sample(&mut groups, &mut metric_names, &sample)?;
    }

    let mixed_histogram_keys = find_mixed_histogram_keys(&native_samples, &groups, &metric_names);
    let mut out = Vec::new();
    for entry in native_samples {
        let key = entry.0;
        let sample = entry.1;
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        let metric = metric_names.get(&key).map_or("", String::as_str);
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = sample.labels.clone();
            labels.insert(label_name, format_quantile_label(*quantile));
            InstantSample {
                labels,
                ts_ms: sample.timestamp_ms,
                value: SampleValue::Float(native_histogram_quantile(
                    *quantile,
                    &sample.histogram,
                    metric,
                )),
                drop_name: true,
            }
        }));
    }
    for entry in groups {
        let key = entry.0;
        let group = entry.1;
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = group.labels.clone();
            let mut buckets = group.buckets.clone();
            labels.insert(label_name, format_quantile_label(*quantile));
            let result = classic_histogram_quantile(*quantile, &mut buckets);
            if result.forced {
                emit_histogram_quantile_forced_monotonicity_info(
                    metric_names.get(&key).map_or("", String::as_str),
                    time_ms,
                    result.repairs,
                );
            }
            InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(result.value),
                drop_name: true,
            }
        }));
    }
    Ok(out)
}

pub(crate) struct ClassicHistogramQuantile {
    pub(crate) value: f64,
    pub(crate) forced: bool,
    pub(crate) repairs: [f64; 3],
}

/// Estimates `quantile` from one series' classic buckets.
///
/// The second return value is whether the buckets had to be forced monotonic;
/// `funcHistogramQuantile` reports that as an info annotation.
pub(crate) fn classic_histogram_quantile(
    quantile: f64,
    buckets: &mut [ClassicBucket],
) -> ClassicHistogramQuantile {
    let NormalizedClassicBuckets {
        buckets,
        forced,
        repairs,
    } = normalized_classic_histogram_buckets(buckets);
    let result = |value| ClassicHistogramQuantile {
        value,
        forced,
        repairs,
    };
    if quantile.is_nan() {
        return result(f64::NAN);
    }
    if quantile < 0.0 {
        return result(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return result(f64::INFINITY);
    }

    if buckets.len() < 2
        || !buckets.last().is_some_and(|bucket| {
            bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
        })
    {
        return result(f64::NAN);
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return result(f64::NAN);
    }
    let rank = quantile * total;
    // The fallback is a permanent mutation survivor because it is never
    // taken: counts are made monotonic above, the last one is the total, and
    // `rank` is at most that total, so the search always finds a bucket.
    let bucket_index = buckets
        .iter()
        .position(|bucket| bucket.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if bucket_index == buckets.len() - 1 {
        return result(buckets[bucket_index - 1].upper_bound);
    }

    let bucket = buckets[bucket_index];
    let mut lower_bound = 0.0;
    let mut previous_count = 0.0;
    if bucket_index == 0 {
        if bucket.upper_bound <= 0.0 {
            return result(bucket.upper_bound);
        }
    } else {
        let previous = buckets[bucket_index - 1];
        lower_bound = previous.upper_bound;
        previous_count = previous.count;
    }

    let bucket_count = bucket.count - previous_count;
    if bucket_count <= 0.0 {
        return result(bucket.upper_bound);
    }
    let value =
        lower_bound + (bucket.upper_bound - lower_bound) * ((rank - previous_count) / bucket_count);
    result(value)
}

struct NativeHistogramBucketWalkEntry {
    position: usize,
    bucket: NativeQuantileBucket,
}

/// Estimates `quantile` from one native histogram.
///
/// This walks the buckets accumulating a rank until it passes `quantile *
/// count`, then interpolates inside the bucket it stopped in. A quantile at or
/// above the median walks from the TOP instead, which is what Prometheus does
/// and which changes the answer in the last bits.
///
/// A histogram whose `sum` is NaN observed a NaN, and those observations raise
/// `count` without landing in any bucket. Such a histogram is always walked
/// forwards, and it reports either an unreachable rank (the result is NaN) or a
/// rank the buckets do not fully account for (the result is skewed high).
pub(crate) fn native_histogram_quantile(
    quantile: f64,
    hist: &NativeHistogram,
    metric: &str,
) -> f64 {
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if matches!(hist.count.classify(), FpCategory::Zero) || quantile.is_nan() {
        return f64::NAN;
    }

    let buckets = native_histogram_all_buckets(hist);
    let forwards = hist.sum.is_nan() || quantile < 0.5;
    let mut rank = if forwards {
        quantile * hist.count
    } else {
        (1.0 - quantile) * hist.count
    };

    let mut walked = 0_usize;
    let mut count = 0.0_f64;
    let mut bucket = NativeQuantileBucket {
        lower: 0.0,
        upper: 0.0,
        count: 0.0,
    };
    for entry in bucket_walk(&buckets, forwards) {
        walked = entry.position;
        let candidate = entry.bucket;
        if candidate.count == 0.0 {
            continue;
        }
        bucket = candidate;
        count += candidate.count;
        if count >= rank {
            break;
        }
    }

    if hist.is_nhcb() {
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            if bucket.upper <= 0.0 {
                return bucket.upper;
            }
            bucket.lower = 0.0;
        } else if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return bucket.lower;
        }
    } else if bucket.lower < 0.0 && bucket.upper > 0.0 {
        bucket = zero_bucket_bounds(hist, bucket);
    }

    // Rounding can push the running count past the histogram's own.
    count = count.min(hist.count);
    if count < rank {
        if hist.sum.is_nan() {
            emit_info(native_histogram_quantile_nan_result_info(metric));
            return f64::NAN;
        }
        return bucket.upper;
    }

    rank = if forwards {
        rank - (count - bucket.count)
    } else {
        count - rank
    };

    if hist.sum.is_nan() {
        let observed = super::observed_histogram_count(count, &buckets, walked);
        if observed < hist.count {
            emit_info(native_histogram_quantile_nan_skew_info(metric));
        }
    }

    native_histogram_bucket_quantile(hist, bucket, rank / bucket.count)
}

/// The buckets to walk in the direction the quantile asks for.
fn bucket_walk(
    buckets: &[NativeQuantileBucket],
    forwards: bool,
) -> Box<dyn Iterator<Item = NativeHistogramBucketWalkEntry> + '_> {
    let entries = (0..buckets.len()).map(|position| NativeHistogramBucketWalkEntry {
        position,
        bucket: buckets[position],
    });
    if forwards {
        Box::new(entries)
    } else {
        Box::new(entries.rev())
    }
}

/// Quantile labels use `OpenMetrics` floats, including a decimal for integers.
#[cfg(test)]
pub(crate) fn format_quantile_label(value: f64) -> String {
    if !value.is_finite() {
        return crate::http_api::format_sample_value(value);
    }
    if value == 0.0 {
        return "0.0".to_owned();
    }
    if !(1e-4..1e6).contains(&value.abs()) {
        let text = format!("{value:e}");
        let exponent_at = text.find('e').expect("scientific float has an exponent");
        let mantissa = &text[..exponent_at];
        let exponent = &text[exponent_at + 1..];
        let exponent = exponent
            .parse::<i32>()
            .expect("float exponent is an integer");
        return format!("{mantissa}e{exponent:+03}");
    }
    let mut text = value.to_string();
    if !text.contains('.') {
        text.push_str(".0");
    }
    text
}

#[cfg(test)]
mod tests {
    #[yare::parameterized(
        zero = { 0.0, "0.0" },
        negative_zero = { -0.0, "0.0" },
        one = { 1.0, "1.0" },
        negative_one = { -1.0, "-1.0" },
        fraction = { 0.25, "0.25" },
        small_scientific = { 1e-6, "1e-06" },
        large_scientific = { 1e6, "1e+06" },
        nan = { f64::NAN, "NaN" },
        infinity = { f64::INFINITY, "+Inf" },
    )]
    fn quantile_labels_preserve_openmetrics_float_contract(value: f64, expected: &str) {
        assert2::assert!(crate::engine::histogram::format_quantile_label(value) == expected);
    }
}
