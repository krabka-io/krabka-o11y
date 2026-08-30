use super::*;

/// Merge a series into the accumulator.
///
/// This unions by label set, sums samples at equal timestamps, and
/// concatenates exemplars. The querier emits a series' labels in a
/// deterministic order for a given group, so equal label sets across shards
/// compare equal as vectors.
pub fn merge_metric_series(acc: &mut Vec<MetricSeries>, incoming: MetricSeries) {
    let Some(existing) = acc.iter_mut().find(|s| s.labels == incoming.labels) else {
        acc.push(incoming);
        return;
    };
    merge_samples(&mut existing.samples, incoming.samples);
    existing.exemplars.extend(incoming.exemplars);
    // `prom_labels` is derived from the (matching) label set, so the existing
    // series already carries the correct value.
}
