use super::*;

pub(crate) fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for (labels, values) in block_samples {
        let target = samples.entry(labels).or_default();
        for (timestamp_ns, value) in values {
            let sample = target.entry(timestamp_ns).or_default();
            sample.merge(value);
        }
    }
}
