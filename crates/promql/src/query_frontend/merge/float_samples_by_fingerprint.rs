use super::{BTreeMap, RangeSeries, SampleValue, SeriesFingerprint};

pub(crate) fn float_samples_by_fingerprint(
    series: Vec<RangeSeries>,
) -> BTreeMap<SeriesFingerprint, BTreeMap<i64, f64>> {
    let mut samples_by_fingerprint = BTreeMap::new();
    for series in series {
        let mut samples_by_timestamp = BTreeMap::new();
        for sample in series.samples {
            if let SampleValue::Float(value) = sample.value {
                samples_by_timestamp.insert(sample.timestamp_ms, value);
            }
        }
        samples_by_fingerprint.insert(series.labels.fingerprint(), samples_by_timestamp);
    }
    samples_by_fingerprint
}
