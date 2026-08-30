//! Prometheus `remote_write` sink implementation.

use async_trait::async_trait;
use prost::Message as _;

use crate::metricsgen::{
    BucketSpan, NativeHistogram,
    series::{Exemplar, Series, SeriesPayload, SeriesSample},
    sink::{RemoteWriteSink, SinkError},
};

#[cfg(test)]
mod tests {

    use prost::Message as _;

    use super::*;
    use crate::metricsgen::{
        BucketSpan, NativeHistogram,
        series::{Exemplar, Series, SeriesSample},
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestWriteRequest {
        #[prost(message, repeated, tag = "1")]
        timeseries: Vec<TestTimeSeries>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestTimeSeries {
        #[prost(message, repeated, tag = "1")]
        labels: Vec<TestLabel>,
        #[prost(message, repeated, tag = "2")]
        samples: Vec<TestSample>,
        #[prost(message, repeated, tag = "3")]
        exemplars: Vec<TestExemplar>,
        #[prost(message, repeated, tag = "4")]
        histograms: Vec<TestHistogram>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestLabel {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(string, tag = "2")]
        value: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestSample {
        #[prost(double, tag = "1")]
        value: f64,
        #[prost(int64, tag = "2")]
        timestamp: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestExemplar {
        #[prost(message, repeated, tag = "1")]
        labels: Vec<TestLabel>,
        #[prost(double, tag = "2")]
        value: f64,
        #[prost(int64, tag = "3")]
        timestamp: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestBucketSpan {
        #[prost(sint32, tag = "1")]
        offset: i32,
        #[prost(uint32, tag = "2")]
        length: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestHistogram {
        #[prost(double, tag = "2")]
        count_float: f64,
        #[prost(double, tag = "3")]
        sum: f64,
        #[prost(sint32, tag = "4")]
        schema: i32,
        #[prost(double, tag = "5")]
        zero_threshold: f64,
        #[prost(double, tag = "7")]
        zero_count_float: f64,
        #[prost(message, repeated, tag = "11")]
        positive_spans: Vec<TestBucketSpan>,
        #[prost(double, repeated, tag = "13")]
        positive_counts: Vec<f64>,
        #[prost(enumeration = "TestResetHint", tag = "14")]
        reset_hint: i32,
        #[prost(int64, tag = "15")]
        timestamp: i64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    enum TestResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }

    fn has_label(ts: &WireTimeSeries, k: &str, v: &str) -> bool {
        ts.labels.iter().any(|(lk, lv)| lk == k && lv == v)
    }

    #[test]
    fn encode_write_request_emits_snappy_protobuf_remote_write() {
        let rows = vec![WireTimeSeries {
            labels: vec![
                ("__name__".into(), "traces_spanmetrics_calls_total".into()),
                ("service".into(), "api".into()),
            ],
            value: 7.0,
            timestamp_ms: 1_234,
            exemplars: vec![Exemplar {
                value: 0.042,
                labels: vec![("trace_id".into(), "0abc".into())],
                timestamp_ms: 1_235,
            }],
            native_histogram: None,
        }];

        let compressed = encode_write_request(&rows).unwrap();
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let request = TestWriteRequest::decode(decoded.as_slice()).unwrap();

        assert2::assert!(
            request
                == TestWriteRequest {
                    timeseries: vec![TestTimeSeries {
                        labels: vec![
                            TestLabel {
                                name: "__name__".into(),
                                value: "traces_spanmetrics_calls_total".into(),
                            },
                            TestLabel {
                                name: "service".into(),
                                value: "api".into(),
                            },
                        ],
                        samples: vec![TestSample {
                            value: 7.0,
                            timestamp: 1_234,
                        }],
                        exemplars: vec![TestExemplar {
                            labels: vec![TestLabel {
                                name: "trace_id".into(),
                                value: "0abc".into(),
                            }],
                            value: 0.042,
                            timestamp: 1_235,
                        }],
                        histograms: vec![],
                    }],
                }
        );
    }

    #[test]
    fn counter_becomes_one_timeseries_with_name_label() {
        let s = Series {
            name: "traces_spanmetrics_calls_total".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::Counter(3.0),
            exemplars: vec![],
            timestamp_ms: 1_000,
        };

        let out = to_timeseries(&[s]);

        assert2::assert!(
            out == vec![WireTimeSeries {
                labels: vec![
                    ("__name__".into(), "traces_spanmetrics_calls_total".into()),
                    ("service".into(), "api".into()),
                ],
                value: 3.0,
                timestamp_ms: 1_000,
                exemplars: vec![],
                native_histogram: None,
            }]
        );
    }

    #[test]
    fn classic_histogram_fans_into_bucket_sum_count() {
        let s = Series {
            name: "traces_spanmetrics_latency".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::ClassicHistogram {
                buckets: vec![(0.004, 0.0), (0.008, 2.0)],
                sum: 0.012,
                count: 2.0,
            },
            exemplars: vec![Exemplar {
                value: 0.005,
                labels: vec![("trace_id".into(), "ab".into())],
                timestamp_ms: 1_000,
            }],
            timestamp_ms: 1_000,
        };

        let out = to_timeseries(&[s]);

        assert2::assert!(out.len() == 5);
        let bucket_inf = out
            .iter()
            .find(|t| {
                has_label(t, "__name__", "traces_spanmetrics_latency_bucket")
                    && has_label(t, "le", "+Inf")
            })
            .unwrap();
        assert2::assert!((bucket_inf.value - 2.0).abs() < 1e-9);

        let sum = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_sum"))
            .unwrap();
        assert2::assert!((sum.value - 0.012).abs() < 1e-9);

        let count = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_count"))
            .unwrap();
        assert2::assert!((count.value - 2.0).abs() < 1e-9);

        let le_8 = out
            .iter()
            .find(|t| {
                has_label(t, "__name__", "traces_spanmetrics_latency_bucket")
                    && has_label(t, "le", "0.008")
            })
            .unwrap();
        assert2::assert!(le_8.exemplars.len() == 1);
        assert2::assert!(le_8.exemplars[0].labels[0].0.as_str() == "trace_id");
    }

    #[test]
    fn native_histogram_encodes_remote_write_histogram() {
        let rows = to_timeseries(&[Series {
            name: "traces_spanmetrics_latency".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::NativeHistogram(NativeHistogram {
                schema: 8,
                zero_threshold: 0.001,
                zero_count: 1.5,
                count: 4.5,
                sum: 0.25,
                positive_spans: vec![BucketSpan {
                    offset: -2,
                    length: 2,
                }],
                positive_counts: vec![2.0, 1.0],
            }),
            exemplars: vec![Exemplar {
                value: 0.12,
                labels: vec![("trace_id".into(), "abc".into())],
                timestamp_ms: 1_235,
            }],
            timestamp_ms: 1_234,
        }]);

        let compressed = encode_write_request(&rows).unwrap();
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let request = TestWriteRequest::decode(decoded.as_slice()).unwrap();

        assert2::assert!(
            request
                == TestWriteRequest {
                    timeseries: vec![TestTimeSeries {
                        labels: vec![
                            TestLabel {
                                name: "__name__".into(),
                                value: "traces_spanmetrics_latency".into(),
                            },
                            TestLabel {
                                name: "service".into(),
                                value: "api".into(),
                            },
                        ],
                        samples: vec![],
                        exemplars: vec![TestExemplar {
                            labels: vec![TestLabel {
                                name: "trace_id".into(),
                                value: "abc".into(),
                            }],
                            value: 0.12,
                            timestamp: 1_235,
                        }],
                        histograms: vec![TestHistogram {
                            count_float: 4.5,
                            sum: 0.25,
                            schema: 8,
                            zero_threshold: 0.001,
                            zero_count_float: 1.5,
                            positive_spans: vec![TestBucketSpan {
                                offset: -2,
                                length: 2,
                            }],
                            positive_counts: vec![2.0, 1.0],
                            reset_hint: TestResetHint::No as i32,
                            timestamp: 1_234,
                        }],
                    }],
                }
        );
    }

    #[test]
    fn le_label_renders_inf_and_floats() {
        assert2::assert!(le_label(f64::INFINITY) == "+Inf");
        assert2::assert!(le_label(0.008) == "0.008");
    }
}

mod bucket_exemplars;
mod bucket_spans_to_proto;
mod encode_write_request;
mod histogram;
mod histograms_to_proto;
mod label;
mod labels_to_proto;
mod le_label;
mod prometheus_remote_write_sink;
mod push_classic_histogram;
mod remote_write_bucket_span;
mod remote_write_exemplar;
mod reset_hint;
mod sample;
mod samples_to_proto;
mod time_series;
mod to_timeseries;
mod wire_time_series;
mod with_name;
mod write_request;

use bucket_exemplars::bucket_exemplars;
use bucket_spans_to_proto::bucket_spans_to_proto;
use encode_write_request::encode_write_request;
use histogram::Histogram;
use histograms_to_proto::histograms_to_proto;
use label::Label;
use labels_to_proto::labels_to_proto;
pub use le_label::le_label;
pub use prometheus_remote_write_sink::PrometheusRemoteWriteSink;
use push_classic_histogram::push_classic_histogram;
use remote_write_bucket_span::RemoteWriteBucketSpan;
use remote_write_exemplar::RemoteWriteExemplar;
use reset_hint::ResetHint;
use sample::Sample;
use samples_to_proto::samples_to_proto;
use time_series::TimeSeries;
pub use to_timeseries::to_timeseries;
pub use wire_time_series::WireTimeSeries;
use with_name::with_name;
use write_request::WriteRequest;
