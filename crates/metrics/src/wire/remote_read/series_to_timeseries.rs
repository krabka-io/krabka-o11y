use super::{Labels, v1};

#[must_use]
pub fn series_to_timeseries(series: Vec<(Labels, Vec<(i64, f64)>)>) -> v1::QueryResult {
    let mut timeseries = series
        .into_iter()
        .map(|(labels, samples)| {
            let mut labels = labels
                .iter()
                .map(|(name, value)| v1::Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            labels.sort_by(|left, right| left.name.cmp(&right.name));

            let mut samples = samples
                .into_iter()
                .map(|(timestamp, value)| v1::Sample { value, timestamp })
                .collect::<Vec<_>>();
            samples.sort_by_key(|sample| sample.timestamp);

            v1::TimeSeries {
                labels,
                samples,
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    timeseries.sort_by(|left, right| {
        left.labels
            .iter()
            .map(|label| (&label.name, &label.value))
            .cmp(right.labels.iter().map(|label| (&label.name, &label.value)))
    });
    v1::QueryResult { timeseries }
}
