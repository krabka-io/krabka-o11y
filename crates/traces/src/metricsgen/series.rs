//! Neutral series payload model consumed by `remote_write` sinks.

use crate::metricsgen::NativeHistogram;

/// Prometheus exemplar attached to a sample.
#[derive(Clone, Debug, PartialEq)]
pub struct Exemplar {
    pub value: f64,
    pub labels: Vec<krabka_traceql::MetricLabel>,
    pub timestamp_ms: i64,
}

/// Batch of series for one tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesPayload {
    pub tenant: String,
    pub series: Vec<Series>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBucket {
    pub upper_bound: f64,
    pub cumulative_count: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramSummary {
    pub buckets: Vec<HistogramBucket>,
    pub sum: f64,
    pub count: f64,
}

/// Neutral sample shape emitted by metrics-generator processors.
#[derive(Clone, Debug, PartialEq)]
pub enum SeriesSample {
    Counter(f64),
    Gauge(f64),
    ClassicHistogram {
        buckets: Vec<HistogramBucket>,
        sum: f64,
        count: f64,
    },
    NativeHistogram(NativeHistogram),
}

/// One named Prometheus series without the `__name__` label.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub name: String,
    pub labels: Vec<krabka_traceql::MetricLabel>,
    pub sample: SeriesSample,
    pub exemplars: Vec<Exemplar>,
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn sorted_labels_orders_by_name_then_value() {
        let labels = sorted_labels(vec![
            krabka_traceql::MetricLabel::new("service", "checkout"),
            krabka_traceql::MetricLabel::new("span_kind", "server"),
            krabka_traceql::MetricLabel::new("service", "api"),
        ]);

        assert2::assert!(
            labels
                == vec![
                    krabka_traceql::MetricLabel::new("service", "api"),
                    krabka_traceql::MetricLabel::new("service", "checkout"),
                    krabka_traceql::MetricLabel::new("span_kind", "server"),
                ]
        );
    }

    #[test]
    fn series_payload_carries_histogram_and_exemplars() {
        let payload = SeriesPayload {
            tenant: "acme".into(),
            series: vec![Series {
                name: "traces_spanmetrics_latency".into(),
                labels: sorted_labels(vec![krabka_traceql::MetricLabel::new(
                    "service", "checkout",
                )]),
                sample: SeriesSample::NativeHistogram(NativeHistogram {
                    schema: 8,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count: 1.0,
                    sum: 0.25,
                    positive_spans: Vec::new(),
                    positive_counts: Vec::new(),
                }),
                exemplars: vec![Exemplar {
                    value: 0.25,
                    labels: sorted_labels(vec![krabka_traceql::MetricLabel::new("trace_id", "01")]),
                    timestamp_ms: 123,
                }],
                timestamp_ms: 123,
            }],
        };

        assert2::assert!(payload.tenant.as_str() == "acme");
        assert2::assert!((payload.series[0].exemplars[0].value - 0.25).abs() < 1e-9);
    }
}

/// Sort labels into a deterministic order for the encoder and the tests.
#[must_use]
pub fn sorted_labels(
    mut labels: Vec<krabka_traceql::MetricLabel>,
) -> Vec<krabka_traceql::MetricLabel> {
    labels.sort();
    labels
}
