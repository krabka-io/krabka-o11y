use super::{BTreeMap, RangeSeries, SampleValue, SeriesFingerprint};

pub(crate) fn float_samples_by_fingerprint(
    series: Vec<RangeSeries>,
) -> BTreeMap<SeriesFingerprint, BTreeMap<i64, f64>> {
    series
        .into_iter()
        .map(|series| {
            (
                series.labels.fingerprint(),
                series
                    .samples
                    .into_iter()
                    .filter_map(|(ts_ms, value)| match value {
                        SampleValue::Float(value) => Some((ts_ms, value)),
                        SampleValue::Histogram(_) => None,
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect()
}
