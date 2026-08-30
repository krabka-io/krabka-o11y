//! Span-metrics RED processor.

use std::collections::{HashMap, HashSet};

use krabka_units::convert::ByteSizeExt as _;
use num_traits::ToPrimitive as _;

use crate::metricsgen::{
    config::MetricsGenConfig,
    contract::{SpanKind, SpanRecord, StatusCode},
    series::{Exemplar, Series, SeriesSample, sorted_labels},
};

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::ByteSize;

    use super::*;
    use crate::metricsgen::{
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
        series::{Series, SeriesSample},
    };

    /// `dimension_labels` builds the four label pairs a span is aggregated
    /// under, sorted by name. Each value differs from every other, so a pair
    /// reading its neighbour's field still produces a well-formed label and is
    /// caught only by the values disagreeing.
    #[test]
    fn span_dimensions_carry_four_sorted_pairs() {
        let record = span(
            "checkout",
            "GET /orders",
            SpanKind::Client,
            StatusCode::Error,
            5,
            1,
        );

        let labels = super::dimension_labels(&record);
        check!(
            labels
                == vec![
                    ("service".to_string(), "checkout".to_string()),
                    ("span_kind".to_string(), "SPAN_KIND_CLIENT".to_string()),
                    ("span_name".to_string(), "GET /orders".to_string()),
                    ("status_code".to_string(), "STATUS_CODE_ERROR".to_string()),
                ],
            "got {labels:?}"
        );

        // Sorted by name, not by the order they are written in the source: the
        // kind comes before the name alphabetically though it is built after.
        let names: Vec<&str> = labels.iter().map(|(name, _)| name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        check!(names == sorted, "labels are not sorted: {names:?}");

        // A different kind and status reach different values, so neither is a
        // constant.
        let other = span("api", "POST /x", SpanKind::Server, StatusCode::Ok, 5, 1);
        let other = super::dimension_labels(&other);
        check!(other[1].1 == "SPAN_KIND_SERVER");
        check!(other[3].1 == "STATUS_CODE_OK");
        check!(other[0].1 == "api", "the service follows its span");
        check!(other[2].1 == "POST /x");
    }

    fn span(
        service: &str,
        name: &str,
        kind: SpanKind,
        status: StatusCode,
        dur_ns: i64,
        size: u64,
    ) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: [0; 8],
            name: name.into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            size: ByteSize::from_bytes(size),
        }
    }

    fn span_with_status_message(message: &str) -> SpanRecord {
        SpanRecord {
            status_message: message.into(),
            ..span(
                "api",
                "GET /x",
                SpanKind::Server,
                StatusCode::Error,
                5_000_000,
                1,
            )
        }
    }

    fn find<'a>(series: &'a [Series], name: &str, span_name: &str) -> &'a Series {
        series
            .iter()
            .find(|s| {
                s.name == name
                    && s.labels
                        .iter()
                        .any(|(k, v)| k == "span_name" && v == span_name)
            })
            .unwrap_or_else(|| panic!("no {name} for {span_name}"))
    }

    #[test]
    fn red_counts_calls_and_size_per_dimension() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            100,
        ));
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            7_000_000,
            150,
        ));
        reg.record_span(&span(
            "api",
            "GET /y",
            SpanKind::Server,
            StatusCode::Error,
            3_000_000,
            50,
        ));

        let out = reg.drain(1_000);

        let calls_x = find(&out, "traces_spanmetrics_calls_total", "GET /x");
        assert2::assert!(
            matches!(calls_x.sample, SeriesSample::Counter(c) if (c - 2.0).abs() < 1e-9)
        );
        let size_x = find(&out, "traces_spanmetrics_size_total", "GET /x");
        assert2::assert!(
            matches!(size_x.sample, SeriesSample::Counter(c) if (c - 250.0).abs() < 1e-9)
        );

        assert2::assert!(
            calls_x.labels
                == vec![
                    ("service".to_string(), "api".to_string()),
                    ("span_kind".to_string(), "SPAN_KIND_SERVER".to_string()),
                    ("span_name".to_string(), "GET /x".to_string()),
                    ("status_code".to_string(), "STATUS_CODE_OK".to_string()),
                ]
        );

        let calls_y = find(&out, "traces_spanmetrics_calls_total", "GET /y");
        assert2::assert!(
            matches!(calls_y.sample, SeriesSample::Counter(c) if (c - 1.0).abs() < 1e-9)
        );
    }

    #[test]
    fn status_message_dimension_is_opt_in() {
        let cfg = MetricsGenConfig {
            enable_status_message: true,
            ..MetricsGenConfig::default()
        };
        let mut reg = SpanMetricsRegistry::new(&cfg);
        reg.record_span(&span_with_status_message("deadline exceeded"));

        let out = reg.drain(1_000);
        let calls = find(&out, "traces_spanmetrics_calls_total", "GET /x");

        assert2::assert!(
            calls
                .labels
                .iter()
                .any(|(k, v)| k == "status_message" && v == "deadline exceeded")
        );
    }

    #[test]
    fn latency_histogram_buckets_and_sum() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            7_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        match &lat.sample {
            SeriesSample::ClassicHistogram {
                buckets,
                sum,
                count,
            } => {
                assert2::assert!((*count - 2.0).abs() < 1e-9);
                assert2::assert!((*sum - 0.012).abs() < 1e-6);
                let le_8ms = buckets
                    .iter()
                    .find(|(le, _)| (*le - 0.008).abs() < 1e-9)
                    .unwrap();
                assert2::assert!((le_8ms.1 - 2.0).abs() < 1e-9);
                let le_4ms = buckets
                    .iter()
                    .find(|(le, _)| (*le - 0.004).abs() < 1e-9)
                    .unwrap();
                assert2::assert!(le_4ms.1.abs() < 1e-9);
            }
            other => panic!("expected ClassicHistogram, got {other:?}"),
        }
    }

    /// An exemplar carries the span's start time in milliseconds and its
    /// duration in seconds. The shared fixture starts every span at zero,
    /// where dividing, multiplying and taking a remainder all give zero, so
    /// the timestamp conversion was untested however many exemplar tests ran.
    /// Both numbers are given values that tell the three operations apart.
    #[test]
    fn an_exemplar_converts_start_to_millis_and_duration_to_seconds() {
        let cfg = MetricsGenConfig {
            max_exemplars_per_series: 2,
            ..MetricsGenConfig::default()
        };
        let mut reg = SpanMetricsRegistry::new(&cfg);
        let mut record = span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            2_500_000_000,
            1,
        );
        // 1.5s in, so the timestamp is 1500ms: multiplying instead gives
        // 1.5e15 and a remainder gives 0, and neither passes for 1500.
        record.start_ns = 1_500_000_000;
        reg.record_span(&record);

        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert2::check!(lat.exemplars.len() == 1);
        let exemplar = &lat.exemplars[0];
        assert2::check!(exemplar.timestamp_ms == 1_500, "milliseconds, not nanos");
        // 2.5s likewise: a remainder gives 5e8 and a product gives 2.5e18.
        assert2::check!(
            (exemplar.value - 2.5).abs() < 1e-9,
            "seconds, not nanos or a remainder"
        );
    }

    #[test]
    fn exemplar_carries_trace_id_when_enabled() {
        let cfg = MetricsGenConfig {
            max_exemplars_per_series: 2,
            ..MetricsGenConfig::default()
        };
        let mut reg = SpanMetricsRegistry::new(&cfg);
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert2::assert!(lat.exemplars.len() == 1);
        let ex = &lat.exemplars[0];
        assert2::assert!(
            ex.labels
                .iter()
                .any(|(k, v)| { k == "trace_id" && v == "abababababababababababababababab" })
        );
        assert2::assert!((ex.value - 0.005).abs() < 1e-6);
    }

    #[test]
    fn exemplars_off_by_default() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert2::assert!(lat.exemplars.is_empty());
    }

    #[test]
    fn drain_emits_cumulative_counters() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        let mk = || {
            span(
                "api",
                "GET /x",
                SpanKind::Server,
                StatusCode::Ok,
                5_000_000,
                1,
            )
        };

        reg.record_span(&mk());
        let first = reg.drain(1_000);
        assert2::assert!(matches!(
            find(&first, "traces_spanmetrics_calls_total", "GET /x").sample,
            SeriesSample::Counter(c) if (c - 1.0).abs() < 1e-9
        ));

        // Next interval, one more call: the `_total` counter is CUMULATIVE
        // (monotonic), not a per-interval delta — it must read 2, not reset to 1.
        reg.record_span(&mk());
        let second = reg.drain(2_000);
        assert2::assert!(matches!(
            find(&second, "traces_spanmetrics_calls_total", "GET /x").sample,
            SeriesSample::Counter(c) if (c - 2.0).abs() < 1e-9
        ));

        // An interval with no new spans still emits the running total (so PromQL
        // never sees a spurious counter reset), and the latency histogram count
        // stays cumulative too.
        let third = reg.drain(3_000);
        check!(!third.is_empty());
        check!(matches!(
            find(&third, "traces_spanmetrics_calls_total", "GET /x").sample,
            SeriesSample::Counter(c) if (c - 2.0).abs() < 1e-9
        ));
        check!(matches!(
            find(&third, "traces_spanmetrics_latency", "GET /x").sample,
            SeriesSample::ClassicHistogram { count, .. } if (count - 2.0).abs() < 1e-9
        ));
    }
}

// === split-modules: generated submodules ===
mod dim_entry;
mod dim_key;
mod dimension_labels;
mod duration_as_f64;
mod latency_histogram;
mod ns_per_sec;
mod span_kind_dim;
mod span_metrics_registry;
mod status_dim;

use dim_entry::DimEntry;
use dim_key::DimKey;
use dim_key::dim_key;
pub use dimension_labels::dimension_labels;
use duration_as_f64::duration_as_f64;
use latency_histogram::LatencyHistogram;
use ns_per_sec::NS_PER_SEC;
use span_kind_dim::span_kind_dim;
pub use span_metrics_registry::SpanMetricsRegistry;
use status_dim::status_dim;
