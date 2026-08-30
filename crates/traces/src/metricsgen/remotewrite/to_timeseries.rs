use super::{Series, WireTimeSeries, SeriesSample, with_name, push_classic_histogram};

#[must_use]
pub fn to_timeseries(series: &[Series]) -> Vec<WireTimeSeries> {
    let mut out = Vec::new();
    for s in series {
        match &s.sample {
            SeriesSample::Counter(value) | SeriesSample::Gauge(value) => {
                out.push(WireTimeSeries {
                    labels: with_name(&s.name, &s.labels),
                    value: *value,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: s.exemplars.clone(),
                    native_histogram: None,
                });
            }
            SeriesSample::ClassicHistogram {
                buckets,
                sum,
                count,
            } => {
                push_classic_histogram(&mut out, s, buckets, *sum, *count);
            }
            SeriesSample::NativeHistogram(histogram) => {
                out.push(WireTimeSeries {
                    labels: with_name(&s.name, &s.labels),
                    value: 0.0,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: s.exemplars.clone(),
                    native_histogram: Some(histogram.clone()),
                });
            }
        }
    }
    out
}
