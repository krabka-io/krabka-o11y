#[cfg(feature = "experimental-functions")]
use super::{
    BTreeMap, ClassicBucketGroups, InstantSample, Result, SampleValue, classic_histogram_quantile,
    emit_histogram_quantile_forced_monotonicity_info, emit_warning, find_mixed_histogram_keys,
    format_quantile_label, group_classic_bucket_sample, invalid_quantile_warning,
    is_valid_quantile, labels_key, labels_without_metric_name, native_histogram_quantile,
    record_metric_name,
};

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
                (labels, sample.ts_ms, histogram.clone()),
            );
            continue;
        }
        group_classic_bucket_sample(&mut groups, &mut metric_names, &sample)?;
    }

    let mixed_histogram_keys = find_mixed_histogram_keys(&native_samples, &groups, &metric_names);
    let mut out = Vec::new();
    for (key, (labels, ts_ms, histogram)) in native_samples {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        let metric = metric_names.get(&key).map_or("", String::as_str);
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            labels.insert(label_name, format_quantile_label(*quantile));
            InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(native_histogram_quantile(*quantile, &histogram, metric)),
                drop_name: true,
            }
        }));
    }
    for (key, (labels, buckets)) in groups {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            let mut buckets = buckets.clone();
            labels.insert(label_name, format_quantile_label(*quantile));
            let (value, forced, repairs) = classic_histogram_quantile(*quantile, &mut buckets);
            if forced {
                emit_histogram_quantile_forced_monotonicity_info(
                    metric_names.get(&key).map_or("", String::as_str),
                    time_ms,
                    repairs,
                );
            }
            InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(value),
                drop_name: true,
            }
        }));
    }
    Ok(out)
}
