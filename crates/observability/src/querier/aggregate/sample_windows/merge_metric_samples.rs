use super::MetricSamples;

pub(crate) fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for series in block_samples {
        let target = samples.entry(series.0).or_default();
        for sample_entry in series.1 {
            let sample = target.entry(sample_entry.0).or_default();
            sample.merge(sample_entry.1);
        }
    }
}
