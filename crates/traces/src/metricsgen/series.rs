//! Neutral series payload model consumed by `remote_write` sinks.

use crate::metricsgen::NativeHistogram;

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

mod exemplar;
mod series_payload;
mod series_sample;
mod series_type;
mod sorted_labels;

pub use exemplar::Exemplar;
pub use series_payload::SeriesPayload;
pub use series_sample::{HistogramBucket, HistogramSummary, SeriesSample};
pub use series_type::Series;
pub use sorted_labels::sorted_labels;
