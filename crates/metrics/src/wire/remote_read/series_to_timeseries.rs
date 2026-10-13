use super::{Labels, v1};

/// One float sample in a remote read series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RemoteReadSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A label set and its float samples for a remote read response.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteReadSeries {
    pub labels: Labels,
    pub samples: Vec<RemoteReadSample>,
}

fn compare_labels(left: &[v1::Label], right: &[v1::Label]) -> std::cmp::Ordering {
    for index in 0..left.len().min(right.len()) {
        let ordering = left[index]
            .name
            .cmp(&right[index].name)
            .then_with(|| left[index].value.cmp(&right[index].value));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

#[must_use]
pub fn series_to_timeseries(series: Vec<RemoteReadSeries>) -> v1::QueryResult {
    let mut timeseries = series
        .into_iter()
        .map(|series| {
            let labels = series.labels;
            let mut labels = labels
                .iter()
                .map(|pair| v1::Label {
                    name: pair.name.to_owned(),
                    value: pair.value.to_owned(),
                })
                .collect::<Vec<_>>();
            labels.sort_by(|left, right| left.name.cmp(&right.name));

            let mut samples = series
                .samples
                .into_iter()
                .map(|sample| v1::Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_ms,
                })
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
    timeseries.sort_by(|left, right| compare_labels(&left.labels, &right.labels));
    v1::QueryResult { timeseries }
}
