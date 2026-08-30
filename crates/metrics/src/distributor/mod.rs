//! Metrics distributor role. It validates a request, applies HA deduplication,
//! and appends to the WAL.

pub mod ha;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes as BodyBytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
pub use ha::{
    DEFAULT_HA_FAILOVER_TIMEOUT, HA_TRACKER_TOPIC, HaDecision, HaElection, HaElectionRecord,
    HaTracker, ha_decision, ha_election, strip_replica_label,
};
use krabka_blockstore::SeriesFingerprint;
use krabka_client_consumer::{Consumer, ConsumerRecord};
use krabka_client_producer::{Header as ProducerHeader, Producer, ProducerRecord};
use krabka_ids::{Offset, PartitionIndex};
use krabka_telemetry::propagation::current_trace_headers;
use krabka_units::prelude::*;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    metrics::v1::MetricsData,
};
use tokio::net::TcpListener;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use tracing::Instrument as _;

use crate::{
    IngestEnforcer, LimitError, Limits, OverridesProvider,
    metrics::ServiceMetrics,
    otlp::{
        DeltaAccumulator, OtlpError, TranslationStrategy, decode_otlp_stateful,
        decode_otlp_stateful_bytes,
    },
    validate_tenant,
    wal::{ClockReadingPayload, SamplePayload, WAL_TOPIC, WalExemplar, WalRecord, partition_key},
    wire::{
        ClockSyncState, ClockWireError, DecodedClockReading, DecodedExemplar, DecodedSample,
        DecodedSeries, GnssFix, UnixNanos, WireError, WireFormat, WrittenCounts,
        decode_clock_readings, decode_v1, decode_v2, negotiate,
    },
};

#[cfg(test)]
mod tests {

    /// A push failure reaches an HTTP client as a status code and a gRPC
    /// client as a code, and the two mappings are separate pieces of code
    /// over the same errors. Both are pinned per variant, because a variant
    /// that borrows its neighbour's status still produces a valid response
    /// and only the code itself gives it away.
    #[test]
    fn a_push_failure_reaches_http_and_grpc_clients_as_its_own_status() {
        use axum::response::IntoResponse as _;

        use crate::limits::LimitError;

        let rate_limited = || LimitError::IngestionRateExceeded {
            rate: 1.0,
            observed: 2.0,
        };
        let bad_request = || LimitError::MaxSeriesPerUser {
            limit: 1,
            observed: 2,
        };
        let unprocessable = || LimitError::SamplesPerQueryExceeded {
            limit: 1,
            observed: 2,
        };

        // The three-way gRPC mapping. 429 and 500 each have their own code;
        // everything else is an invalid argument, including codes that are
        // neither an obvious client nor server fault.
        let grpc = |http| super::status_from_http_status(http, "boom".to_string()).code();
        check!(grpc(429) == tonic::Code::ResourceExhausted);
        check!(grpc(500) == tonic::Code::Internal);
        check!(grpc(400) == tonic::Code::InvalidArgument);
        check!(grpc(422) == tonic::Code::InvalidArgument);
        check!(
            grpc(200) == tonic::Code::InvalidArgument,
            "even a success code"
        );
        check!(
            super::status_from_http_status(429, "boom".to_string()).message() == "boom",
            "the message is carried through, not replaced"
        );

        // Per-variant gRPC codes.
        let code = |error: &super::PushError| super::status_from_push_error(error).code();
        check!(code(&super::PushError::MissingTenant) == tonic::Code::InvalidArgument);
        check!(
            code(&super::PushError::InvalidTenant("x".to_string())) == tonic::Code::InvalidArgument
        );
        check!(
            code(&super::PushError::TooOldSample {
                timestamp_ms: 1,
                oldest_allowed_ms: 2,
            }) == tonic::Code::InvalidArgument
        );
        check!(code(&super::PushError::Limit(rate_limited())) == tonic::Code::ResourceExhausted);
        check!(code(&super::PushError::Limit(bad_request())) == tonic::Code::InvalidArgument);
        check!(code(&super::PushError::Limit(unprocessable())) == tonic::Code::InvalidArgument);
        check!(
            code(&super::PushError::Produce(super::ProduceError::Append(
                "io".to_string()
            ))) == tonic::Code::Internal,
            "a produce failure is ours, not the client's"
        );

        // Per-variant HTTP statuses, which are a separate mapping over the
        // same errors and disagree with the gRPC one on the 422 case.
        let http = |error: super::PushError| error.into_response().status();
        check!(http(super::PushError::MissingTenant) == axum::http::StatusCode::BAD_REQUEST);
        check!(
            http(super::PushError::InvalidTenant("x".to_string()))
                == axum::http::StatusCode::BAD_REQUEST
        );
        check!(
            http(super::PushError::TooOldSample {
                timestamp_ms: 1,
                oldest_allowed_ms: 2,
            }) == axum::http::StatusCode::BAD_REQUEST
        );
        check!(
            http(super::PushError::Limit(rate_limited()))
                == axum::http::StatusCode::TOO_MANY_REQUESTS
        );
        check!(http(super::PushError::Limit(bad_request())) == axum::http::StatusCode::BAD_REQUEST);
        check!(
            http(super::PushError::Limit(unprocessable()))
                == axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "422 survives the HTTP path though gRPC folds it into invalid-argument"
        );
        check!(
            http(super::PushError::Produce(super::ProduceError::Append(
                "io".to_string()
            ))) == axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// A negative out-of-order window disables the check; a zero window does
    /// not. Zero means no out-of-order tolerance at all, so a sample older
    /// than the newest already seen is rejected -- which is the case that
    /// separates `< ZERO` from `<= ZERO`, since every other window agrees.
    #[test]
    fn a_zero_out_of_order_window_still_rejects_older_samples() {
        use crate::wire::DecodedSample;

        let (state, _sink) = test_state();
        let series = |timestamp_ms: i64| {
            let mut labels = Labels::default();
            labels.insert("__name__", "requests");
            DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(timestamp_ms, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }
        };
        let window = |ms: i64| Limits {
            out_of_order_time_window: Time::from_millis(ms),
            ..Limits::default()
        };

        // A zero window: the first sample sets the mark, and one before it is
        // refused.
        let zero = window(0);
        check!(super::enforce_out_of_order_window(&state, &zero, "t", &[series(1_000)]).is_ok());
        check!(
            super::enforce_out_of_order_window(&state, &zero, "t", &[series(999)]).is_err(),
            "one millisecond earlier is out of order"
        );
        check!(
            super::enforce_out_of_order_window(&state, &zero, "t", &[series(1_000)]).is_ok(),
            "the same timestamp is not older"
        );

        // A positive window admits samples within it and refuses those beyond.
        let (state, _sink) = test_state();
        let ten = window(10_000);
        check!(super::enforce_out_of_order_window(&state, &ten, "t", &[series(100_000)]).is_ok());
        check!(
            super::enforce_out_of_order_window(&state, &ten, "t", &[series(90_000)]).is_ok(),
            "exactly the window back is still allowed"
        );
        check!(
            super::enforce_out_of_order_window(&state, &ten, "t", &[series(89_999)]).is_err(),
            "one millisecond beyond it is not"
        );

        // A negative window disables the check entirely.
        let (state, _sink) = test_state();
        let disabled = window(-1);
        check!(
            super::enforce_out_of_order_window(&state, &disabled, "t", &[series(1_000)]).is_ok()
        );
        check!(
            super::enforce_out_of_order_window(&state, &disabled, "t", &[series(1)]).is_ok(),
            "anything goes when the window is negative"
        );
    }

    /// The per-series sample cap counts samples, histograms and exemplars
    /// together. All three are populated here and the total is placed either
    /// side of the limit, because a sum that subtracts one term instead of
    /// adding it still produces a number -- just a smaller one that slips
    /// under the cap.
    #[test]
    fn the_per_series_sample_cap_counts_all_three_collections() {
        use crate::{
            histogram::NativeHistogram,
            wire::{DecodedExemplar, DecodedSample},
        };

        let histogram = || NativeHistogram {
            schema: 0,
            is_float: false,
            reset_hint: crate::ResetHint::Unknown,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };
        let series = |samples: usize, histograms: usize, exemplars: usize| {
            let mut labels = Labels::default();
            labels.insert("__name__", "requests");
            DecodedSeries {
                labels,
                samples: (0..samples)
                    .map(|i| DecodedSample::new(i64::try_from(i).expect("small"), 1.0))
                    .collect(),
                histograms: (0..histograms)
                    .map(|i| (i64::try_from(i).expect("small"), histogram()))
                    .collect(),
                exemplars: (0..exemplars)
                    .map(|i| DecodedExemplar {
                        labels: Labels::default(),
                        timestamp_ms: i64::try_from(i).expect("small"),
                        value: 1.0,
                    })
                    .collect(),
                metadata: None,
            }
        };
        let limits = super::TenantLimits {
            max_samples_per_series: 6,
            max_series_per_request: 10,
            ..super::TenantLimits::default()
        };

        // Three, two and one make exactly the cap, which is allowed.
        check!(
            super::validate(&[series(3, 2, 1)], &limits).is_ok(),
            "exactly at the cap"
        );

        // One more of any kind is over it, whichever collection grows.
        check!(
            super::validate(&[series(4, 2, 1)], &limits).is_err(),
            "one more sample"
        );
        check!(
            super::validate(&[series(3, 3, 1)], &limits).is_err(),
            "one more histogram"
        );
        check!(
            super::validate(&[series(3, 2, 2)], &limits).is_err(),
            "one more exemplar"
        );

        // Each collection alone reaches the cap on its own terms.
        check!(super::validate(&[series(6, 0, 0)], &limits).is_ok());
        check!(super::validate(&[series(7, 0, 0)], &limits).is_err());
        check!(super::validate(&[series(0, 7, 0)], &limits).is_err());
        check!(super::validate(&[series(0, 0, 7)], &limits).is_err());

        // The series cap is separate and counts series, not samples.
        let many = vec![series(1, 0, 0); 10];
        check!(
            super::validate(&many, &limits).is_ok(),
            "ten series is the cap"
        );
        let too_many = vec![series(1, 0, 0); 11];
        check!(
            super::validate(&too_many, &limits).is_err(),
            "eleven is over it"
        );
    }

    /// `keyed_producer_record` takes its topic from the caller, unlike the WAL
    /// record beside it which fixes one. The partition is still left unset so
    /// the partitioner keys on the record key.
    #[test]
    fn a_keyed_record_takes_the_topic_it_is_given() {
        let record = super::keyed_producer_record(
            "elections".to_string(),
            Bytes::from_static(b"tenant-key"),
            b"payload".to_vec(),
        );

        check!(
            record.topic == "elections",
            "the given topic, not the WAL one"
        );
        check!(record.topic != WAL_TOPIC);
        check!(record.partition == None, "the partitioner must choose");
        check!(record.key.as_deref() == Some(&b"tenant-key"[..]));
        check!(
            record.value.as_deref() == Some(&b"payload"[..]),
            "not the key again"
        );
        check!(record.headers.is_empty(), "no trace context on this path");
    }

    /// `wal_producer_record` shapes one WAL append. The partition is left
    /// unset deliberately: the producer's partitioner keys on the record key,
    /// which is what keeps a series on one partition and in order. Setting a
    /// partition here would silently defeat that, so its absence is asserted
    /// rather than left unmentioned.
    #[test]
    fn a_wal_record_carries_its_key_value_and_headers_without_a_partition() {
        let record = super::wal_producer_record(
            Bytes::from_static(b"series-key"),
            b"payload".to_vec(),
            vec![
                ("traceparent".to_string(), "00-abc-def-01".to_string()),
                ("tracestate".to_string(), "vendor=1".to_string()),
            ],
        );

        check!(record.topic == WAL_TOPIC);
        check!(
            record.partition == None,
            "the partitioner must choose, not this"
        );
        check!(record.key.as_deref() == Some(&b"series-key"[..]));
        check!(
            record.value.as_deref() == Some(&b"payload"[..]),
            "not the key again"
        );

        // Headers keep their order and their pairing; the two values differ so
        // a swap between them is visible.
        check!(record.headers.len() == 2);
        check!(record.headers[0].key == "traceparent");
        check!(record.headers[0].value.as_deref() == Some(&b"00-abc-def-01"[..]));
        check!(record.headers[1].key == "tracestate");
        check!(record.headers[1].value.as_deref() == Some(&b"vendor=1"[..]));

        // No trace context means no headers, rather than empty ones.
        let bare = super::wal_producer_record(Bytes::from_static(b"k"), b"v".to_vec(), Vec::new());
        check!(bare.headers.is_empty());
    }

    /// `decoded_sample_count` totals three collections across every series.
    /// Each carries a different number, so a term dropped from the sum is a
    /// specific shortfall rather than merely a smaller total -- with equal
    /// counts, dropping any one of the three looks the same.
    #[test]
    fn a_decoded_batch_counts_samples_histograms_and_exemplars() {
        use crate::{
            histogram::NativeHistogram,
            wire::{DecodedExemplar, DecodedSample},
        };

        let series = |samples: usize, histograms: usize, exemplars: usize| DecodedSeries {
            labels: Labels::default(),
            samples: (0..samples)
                .map(|i| DecodedSample::new(i64::try_from(i).expect("small"), 1.0))
                .collect(),
            histograms: (0..histograms)
                .map(|i| {
                    (
                        i64::try_from(i).expect("small"),
                        NativeHistogram {
                            schema: 0,
                            is_float: false,
                            reset_hint: crate::ResetHint::Unknown,
                            zero_threshold: 0.0,
                            zero_count: 0.0,
                            count: 0.0,
                            sum: 0.0,
                            positive_spans: Vec::new(),
                            positive_counts: Vec::new(),
                            negative_spans: Vec::new(),
                            negative_counts: Vec::new(),
                            custom_values: None,
                            start_timestamp_ms: None,
                        },
                    )
                })
                .collect(),
            exemplars: (0..exemplars)
                .map(|i| DecodedExemplar {
                    labels: Labels::default(),
                    timestamp_ms: i64::try_from(i).expect("small"),
                    value: 1.0,
                })
                .collect(),
            metadata: None,
        };

        // Three different counts, so dropping any one term is distinguishable
        // from dropping either other.
        check!(super::decoded_sample_count(&[series(3, 5, 7)]) == 15);
        check!(
            super::decoded_sample_count(&[series(3, 0, 0)]) == 3,
            "samples alone"
        );
        check!(
            super::decoded_sample_count(&[series(0, 5, 0)]) == 5,
            "histograms alone"
        );
        check!(
            super::decoded_sample_count(&[series(0, 0, 7)]) == 7,
            "exemplars alone"
        );

        // Several series add up rather than the largest winning.
        check!(super::decoded_sample_count(&[series(3, 0, 0), series(4, 0, 0)]) == 7);

        // Nothing at all is zero, not one.
        check!(super::decoded_sample_count(&[]) == 0);
        check!(
            super::decoded_sample_count(&[series(0, 0, 0)]) == 0,
            "an empty series counts none"
        );
    }

    /// `tenant_limits_to_limits` copies five fields across and leaves the rest
    /// at their defaults. Two of the five are byte sizes and two are counts,
    /// so every value here is distinct: a field reading its neighbour still
    /// produces a well-formed limit, and only distinct values show it.
    #[test]
    fn tenant_limits_map_field_by_field_onto_the_shared_limits() {
        use krabka_units::{bytes, per_sec, secs};

        let tenant = super::TenantLimits {
            max_label_name_len: bytes(11),
            max_label_value_len: bytes(22),
            max_samples_per_series: 33,
            max_series_per_request: 44,
            ingestion_rate: per_sec(55),
            ingestion_burst_size: 66,
            out_of_order_time_window: secs(77),
        };

        let limits = super::tenant_limits_to_limits(&tenant);

        check!(limits.max_label_name_length == bytes(11));
        check!(
            limits.max_label_value_length == bytes(22),
            "not the name length"
        );
        check!(
            limits.ingestion_burst_size == 66,
            "the burst, not a sample count"
        );
        check!(limits.out_of_order_time_window == secs(77));
        check!(limits.ingestion_rate == per_sec(55));

        // The fields with no counterpart keep the shared default rather than
        // picking up a value from the tenant's own limits.
        let defaults = super::Limits::default();
        check!(
            limits.max_global_series_per_user == defaults.max_global_series_per_user,
            "a field with no source stays at its default"
        );
    }
    use std::sync::Mutex;

    fn decoded_series(labels: &[(&str, &str)], samples: usize) -> crate::wire::DecodedSeries {
        let mut set = krabka_blockstore::Labels::new();
        for (name, value) in labels {
            set.insert(*name, *value);
        }
        crate::wire::DecodedSeries {
            labels: set,
            samples: (0..samples)
                .map(|i| crate::wire::DecodedSample {
                    timestamp_ms: i64::try_from(i).unwrap_or(0),
                    value: 1.0,
                    start_timestamp_ms: None,
                })
                .collect(),
            histograms: vec![],
            exemplars: vec![],
            metadata: None,
        }
    }

    /// Every structural limit rejects what exceeds it, so a request sitting
    /// exactly on each one is still accepted. Each is checked at its edge and
    /// one past it, and the errors are matched on their text because the four
    /// differ only in which number they name.
    #[test]
    fn structural_limits_admit_exactly_their_boundary() {
        let limits = TenantLimits {
            max_series_per_request: 2,
            max_samples_per_series: 3,
            max_label_name_len: krabka_units::bytes(4),
            max_label_value_len: krabka_units::bytes(5),
            ..TenantLimits::default()
        };

        let two = [
            decoded_series(&[("ok", "v")], 1),
            decoded_series(&[("ok", "v")], 1),
        ];
        assert!(
            super::validate(&two, &limits).is_ok(),
            "two series fit a limit of two"
        );

        let three = [
            decoded_series(&[("ok", "v")], 1),
            decoded_series(&[("ok", "v")], 1),
            decoded_series(&[("ok", "v")], 1),
        ];
        let err = super::validate(&three, &limits).unwrap_err().to_string();
        assert!(
            err.contains("series per request 3 exceeds limit 2"),
            "got: {err}"
        );

        let at_edge = [decoded_series(&[("ok", "v")], 3)];
        assert!(
            super::validate(&at_edge, &limits).is_ok(),
            "three samples fit a limit of three"
        );
        let over = [decoded_series(&[("ok", "v")], 4)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(
            err.contains("samples per series 4 exceeds limit 3"),
            "got: {err}"
        );

        let at_edge = [decoded_series(&[("abcd", "v")], 1)];
        assert!(
            super::validate(&at_edge, &limits).is_ok(),
            "a four-byte name fits"
        );
        let over = [decoded_series(&[("abcde", "v")], 1)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(
            err.contains("label name length 5 exceeds limit 4"),
            "got: {err}"
        );

        let at_edge = [decoded_series(&[("ok", "vwxyz")], 1)];
        assert!(
            super::validate(&at_edge, &limits).is_ok(),
            "a five-byte value fits"
        );
        let over = [decoded_series(&[("ok", "vwxyz!")], 1)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(
            err.contains("label value length 6 exceeds limit 5"),
            "got: {err}"
        );

        let bad = [decoded_series(&[("has space", "v")], 1)];
        let err = super::validate(&bad, &limits).unwrap_err().to_string();
        assert!(err.contains("invalid label name"), "got: {err}");
    }

    /// The per-series sample budget counts samples, histograms and exemplars
    /// together, so a series can exceed it without any one kind doing so.
    #[test]
    fn the_sample_budget_counts_every_kind_together() {
        let limits = TenantLimits {
            max_samples_per_series: 3,
            ..TenantLimits::default()
        };

        let mut series = decoded_series(&[("ok", "v")], 2);
        series.exemplars = vec![crate::wire::DecodedExemplar {
            labels: krabka_blockstore::Labels::new(),
            value: 1.0,
            timestamp_ms: 1,
        }];
        assert!(
            super::validate(std::slice::from_ref(&series), &limits).is_ok(),
            "two samples and one exemplar is exactly three"
        );

        series.exemplars.push(crate::wire::DecodedExemplar {
            labels: krabka_blockstore::Labels::new(),
            value: 1.0,
            timestamp_ms: 2,
        });
        let err = super::validate(&[series], &limits).unwrap_err().to_string();
        assert!(
            err.contains("samples per series 4 exceeds limit 3"),
            "got: {err}"
        );
    }

    /// The HA election compaction key identifies one tenant-and-cluster pair.
    /// Two pairs sharing a key would let one cluster's election overwrite
    /// another's, so the separator has to do its job.
    #[test]
    fn ha_election_keys_identify_one_tenant_and_cluster() {
        let key = |tenant: &str, cluster: &str| {
            super::ha_election_compaction_key(&crate::distributor::ha::HaElectionRecord {
                tenant: tenant.into(),
                cluster: cluster.into(),
                // Neither is part of the identity: a later election for the
                // same pair replaces the earlier one.
                replica: "replica-1".into(),
                lease_timestamp_ms: 1,
            })
        };

        check!(key("t", "c") == Bytes::from("t\0c"));
        check!(key("t", "c") == key("t", "c"), "the same pair keys alike");
        check!(
            key("t", "c") != key("t", "d"),
            "a different cluster differs"
        );
        check!(key("t", "c") != key("u", "c"), "so does a different tenant");
        check!(
            key("t", "c") != key("tc", ""),
            "the separator stops a shifted split from colliding"
        );
    }

    /// The record both compacted sinks build, checked without a broker.
    #[test]
    fn a_compacted_record_carries_its_topic_key_and_value() {
        let record = super::keyed_producer_record(
            "ha-elections".to_string(),
            Bytes::from_static(b"the-key"),
            b"the-value".to_vec(),
        );

        check!(record.topic == "ha-elections");
        check!(
            record.partition == None,
            "partitioning is left to the producer"
        );
        check!(record.key.as_deref() == Some(&b"the-key"[..]));
        check!(record.value.as_deref() == Some(&b"the-value"[..]));
    }

    /// The WAL record's shape, checked without a broker. The key and value
    /// are distinguishable byte strings so a transposition is visible, and
    /// the absent partition matters: supplying one would override the
    /// producer's key-based partitioner and break per-series ordering.
    #[test]
    fn a_wal_record_carries_its_key_value_and_trace_headers() {
        let record = super::wal_producer_record(
            Bytes::from_static(b"the-key"),
            b"the-value".to_vec(),
            vec![
                ("traceparent".to_string(), "00-abc-def-01".to_string()),
                ("tracestate".to_string(), "vendor=1".to_string()),
            ],
        );

        check!(record.topic == super::WAL_TOPIC);
        check!(
            record.partition == None,
            "partitioning is left to the producer"
        );
        check!(record.key.as_deref() == Some(&b"the-key"[..]));
        check!(record.value.as_deref() == Some(&b"the-value"[..]));
        check!(
            record
                .headers
                .iter()
                .map(|header| (
                    header.key.as_str(),
                    header
                        .value
                        .as_deref()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                ))
                .collect::<Vec<_>>()
                == vec![
                    ("traceparent", Some("00-abc-def-01".to_string())),
                    ("tracestate", Some("vendor=1".to_string())),
                ],
            "headers keep their names, values and order"
        );

        // No active span means no headers, not an empty-valued one.
        let bare = super::wal_producer_record(Bytes::from_static(b"k"), b"v".to_vec(), vec![]);
        check!(bare.headers.is_empty());
    }

    /// The HTTP-to-gRPC mapping the error kinds share. Only two codes get a
    /// status of their own; everything else is the caller's fault by default,
    /// which is the safer reading for a code nobody has mapped yet.
    #[test]
    fn http_statuses_map_to_the_grpc_code_the_client_should_act_on() {
        let map = |code| super::status_from_http_status(code, "why".to_string()).code();

        check!(
            map(429) == tonic::Code::ResourceExhausted,
            "too many requests"
        );
        check!(map(500) == tonic::Code::Internal, "our fault");

        for code in [400, 404, 415, 422, 428, 430, 499, 501, 503] {
            check!(
                map(code) == tonic::Code::InvalidArgument,
                "{code} has no status of its own"
            );
        }

        check!(
            super::status_from_http_status(500, "why".to_string()).message() == "why",
            "the reason is carried through"
        );
    }

    /// Every push failure has to reach the client as the status it should act
    /// on: back off, retry later, or stop sending this request. The table
    /// covers each error kind, including the ones that reach the catch-all,
    /// since that is where a guard that stopped matching would land.
    #[test]
    fn push_errors_reach_the_client_as_the_status_to_act_on() {
        use crate::{limits::LimitError, wire::WireError};

        let cases: Vec<(super::PushError, tonic::Code, &str)> = vec![
            (
                LimitError::IngestionRateExceeded {
                    rate: 1.0,
                    observed: 2.0,
                }
                .into(),
                tonic::Code::ResourceExhausted,
                "a rate limit is a back-off",
            ),
            (
                LimitError::MaxSeriesPerUser {
                    limit: 1,
                    observed: 2,
                }
                .into(),
                tonic::Code::InvalidArgument,
                "a series limit is the request's fault",
            ),
            (
                LimitError::QueryRangeTooLong {
                    limit_secs: 1,
                    observed_secs: 2,
                }
                .into(),
                tonic::Code::InvalidArgument,
                "an unprocessable range is too",
            ),
            (
                super::ProduceError::Append("wal down".into()).into(),
                tonic::Code::Internal,
                "a failed append is ours, not the client's",
            ),
            (
                WireError::UnsupportedContentType("text/plain".into()).into(),
                tonic::Code::InvalidArgument,
                "an undecodable body is the request's fault",
            ),
            (
                WireError::Invalid("bad".into()).into(),
                tonic::Code::InvalidArgument,
                "so is an invalid one",
            ),
            (
                super::PushError::MissingTenant,
                tonic::Code::InvalidArgument,
                "a missing tenant header",
            ),
            (
                super::PushError::InvalidTenant("a/b".into()),
                tonic::Code::InvalidArgument,
                "an unusable tenant header",
            ),
            (
                super::PushError::TooOldSample {
                    timestamp_ms: 1,
                    oldest_allowed_ms: 2,
                },
                tonic::Code::InvalidArgument,
                "a sample the store will not take",
            ),
        ];

        for (error, expected, why) in cases {
            let status = super::status_from_push_error(&error);
            check!(status.code() == expected, "{why}: {error}");
            check!(
                !status.message().is_empty(),
                "{why}: the reason is carried through"
            );
        }
    }

    /// The exemplar codepoint budget is summed across every label name and
    /// value, and compared with a strict `>` so a set landing exactly on the
    /// limit is allowed.
    ///
    /// Two things go wrong quietly here. Read as `>=`, the budget is one
    /// codepoint tighter than documented and an exemplar sitting on the limit
    /// is refused. And the running total is a sum: read as a product, a single
    /// label still totals plausibly while several no longer do, so the check
    /// only misfires once an exemplar carries more than one label.
    #[test]
    fn exemplar_codepoints_are_summed_and_capped_at_the_limit() {
        let exemplar = |pairs: &[(&str, &str)]| {
            let mut labels = krabka_blockstore::Labels::new();
            for (name, value) in pairs {
                labels.insert(*name, *value);
            }
            DecodedExemplar {
                labels,
                timestamp_ms: 0,
                value: 1.0,
            }
        };

        // Eight labels of eight codepoints each side: 128, exactly the budget.
        let owned: Vec<(String, String)> = (0..8)
            .map(|i| (format!("name{i:04}"), format!("valu{i:04}")))
            .collect();
        let at_limit: Vec<(&str, &str)> = owned
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        let total: usize = at_limit
            .iter()
            .map(|(n, v)| n.chars().count() + v.chars().count())
            .sum();
        check!(
            total == MAX_EXEMPLAR_LABEL_CODEPOINTS,
            "fixture is {total}, not the budget"
        );

        check!(
            validate_exemplar_labels(&exemplar(&at_limit)).is_ok(),
            "a set landing exactly on the budget is allowed"
        );

        // One codepoint more, spread across the same number of labels, is not.
        let mut over = at_limit.clone();
        over.push(("x", ""));
        check!(
            validate_exemplar_labels(&exemplar(&over)).is_err(),
            "one codepoint past the budget is refused"
        );

        // One label whose value alone exceeds the budget.
        let long = "v".repeat(MAX_EXEMPLAR_LABEL_CODEPOINTS + 1);
        check!(
            validate_exemplar_labels(&exemplar(&[("trace_id", long.as_str())])).is_err(),
            "single label over the budget"
        );
    }

    /// A push failure's gRPC code tells the client what to do next: back off
    /// (`resource_exhausted`), retry later (`internal`), or stop and fix the
    /// request (`invalid_argument`). The mapping is a chain of match guards on
    /// the underlying HTTP status, and a guard forced either way sends the
    /// wrong instruction -- a rate limit reported as `invalid_argument` makes a
    /// client give up on a request that would succeed after a pause, and a bad
    /// request reported as `resource_exhausted` makes it retry-storm one that
    /// never will.
    #[test]
    fn push_errors_map_to_the_grpc_code_the_client_should_act_on() {
        use crate::limits::LimitError;

        let over_rate = PushError::Limit(LimitError::IngestionRateExceeded {
            rate: 100.0,
            observed: 150.0,
        });
        check!(
            status_from_push_error(&over_rate).code() == tonic::Code::ResourceExhausted,
            "429 limit is resource_exhausted"
        );

        // A 400-class limit is the client's mistake, not a reason to back off.
        let too_many_series = PushError::Limit(LimitError::MaxSeriesPerUser {
            limit: 10,
            observed: 11,
        });
        check!(
            status_from_push_error(&too_many_series).code() == tonic::Code::InvalidArgument,
            "400 limit is invalid_argument"
        );

        let too_long = PushError::Limit(LimitError::LabelNameTooLong {
            limit: 8,
            observed: 9,
        });
        check!(
            status_from_push_error(&too_long).code() == tonic::Code::InvalidArgument,
            "label length is invalid_argument"
        );
    }

    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use krabka_blockstore::Labels;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::{
            ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
            metrics_service_server::MetricsService,
        },
        common::v1::{AnyValue, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Sum, metric, number_data_point,
        },
        resource::v1::Resource,
    };
    use prost::Message;
    use tower::ServiceExt as _;

    use super::*;
    use crate::wire::DecodedSample;

    /// Pins the span-label logic of `tenant_for_span`. A present, non-empty
    /// header goes through verbatim. A missing OR empty `X-Scope-OrgID` falls
    /// back to `"unknown"`. This kills the whole-function replacement mutants,
    /// `"xyzzy"` and `String::new()`, and the `delete !` mutant on
    /// `!value.is_empty()`. The empty-string case maps to `"unknown"` only
    /// while the negation stands.
    #[test]
    fn tenant_for_span_labels_present_and_falls_back_on_missing_or_empty() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert!(tenant_for_span(&present) == "acme");

        let missing = HeaderMap::new();
        assert!(tenant_for_span(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert!(tenant_for_span(&empty) == "unknown");
    }

    #[derive(Default)]
    struct RecordingSink {
        appends: Mutex<Vec<(Bytes, WalRecord)>>,
    }

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .push((key, record));
            Ok(())
        }
    }

    impl RecordingSink {
        fn records(&self) -> Vec<WalRecord> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(_, record)| record.clone())
                .collect()
        }

        fn append_keys(&self) -> Vec<Bytes> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }
    }

    #[derive(Default)]
    struct RecordingHaElectionSink {
        elections: Mutex<Vec<HaElectionRecord>>,
    }

    #[async_trait::async_trait]
    impl HaElectionSink for RecordingHaElectionSink {
        async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .push(record);
            Ok(())
        }
    }

    impl RecordingHaElectionSink {
        fn elections(&self) -> Vec<HaElectionRecord> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .clone()
        }
    }

    struct RecordingHaElectionConsumer {
        batches: Vec<Vec<ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerPoll for RecordingHaElectionConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
            Ok(self.batches.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerCommit for RecordingHaElectionConsumer {
        async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    fn consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: Option<Vec<u8>>,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: value.map(Bytes::from),
            headers: Vec::new(),
        }
    }

    struct FailingHaElectionSink;

    #[async_trait::async_trait]
    impl HaElectionSink for FailingHaElectionSink {
        async fn persist_election(&self, _record: HaElectionRecord) -> Result<(), ProduceError> {
            Err(ProduceError::Append("ha election unavailable".to_string()))
        }
    }

    fn test_state() -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        (Arc::new(DistributorState::new(sink.clone())), sink)
    }

    #[test]
    fn distributor_state_stores_configured_runtime_policy() {
        let sink = Arc::new(RecordingSink::default());
        let state = DistributorState::new(sink)
            .with_ha_failover_timeout(Time::from_millis(-1_000))
            .with_max_rate_buckets(7)
            .with_max_decompressed(kibibytes(64));

        check!(state.ha_failover_timeout == Time::from_millis(-1_000));
        check!(state.ingest_enforcer.max_rate_buckets() == 7);
        check!(state.max_decompressed == kibibytes(64));
    }

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    fn v1_body(labels: Vec<crate::wire::pb::v1::Label>) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels,
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_samples(sample_count: usize) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: (0..sample_count)
                    .map(|index| crate::wire::pb::v1::Sample {
                        value: 1.0,
                        timestamp: i64::try_from(index).expect("test sample index fits in i64"),
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_sample_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            metadata: vec![crate::wire::pb::v1::MetricMetadata {
                r#type: crate::wire::pb::v1::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".into(),
                help: "Total HTTP requests.".into(),
                unit: "requests".into(),
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_label_value(value: &str) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", value)],
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", "abc123")],
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "http_requests_total".into(),
                "Total HTTP requests.".into(),
                "requests".into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                metadata: Some(crate::wire::pb::v2::Metadata {
                    r#type: crate::wire::pb::v2::metadata::MetricType::Counter as i32,
                    help_ref: 3,
                    unit_ref: 4,
                }),
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_ha_replica(replica: &str) -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "cluster".into(),
                "c1".into(),
                "__replica__".into(),
                replica.into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn otlp_body() -> Vec<u8> {
        otlp_gauge_body()
    }

    fn otlp_sum_body(value: f64, timestamp: u64, monotonic: bool, temporality: i32) -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: timestamp,
                                value: Some(number_data_point::Value::AsDouble(value)),
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality,
                            is_monotonic: monotonic,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_gauge_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        description: "CPU utilization ratio.".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_resource_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".into())),
                        }),
                        key_strindex: 0,
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn label(name: &str, value: &str) -> crate::wire::pb::v1::Label {
        crate::wire::pb::v1::Label {
            name: name.into(),
            value: value.into(),
        }
    }

    #[tokio::test]
    async fn push_v1_returns_204_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert_eq!(
            records,
            vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![("__name__".to_string(), "up".to_string())],
                payload: SamplePayload::Float {
                    timestamp_ms: 100,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                exemplars: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn push_v1_accepts_listed_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "identity, snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_v1_accepts_prometheus_remote_write_receiver_path() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/write")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_keys_wal_append_by_tenant_and_series_fingerprint() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert!(records.len() == 1);
        assert!(
            sink.append_keys()
                == vec![crate::wal::partition_key(
                    "tenant-a",
                    records[0].series_fingerprint()
                )]
        );
    }

    #[tokio::test]
    async fn push_v2_sets_written_headers() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == StatusCode::NO_CONTENT);
        check!(
            response
                .headers()
                .get("X-Prometheus-Remote-Write-Samples-Written")
                .and_then(|value| value.to_str().ok())
                == Some("1")
        );
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_v2_preserves_sample_start_timestamp_in_wal() {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 5,
                }],
                ..Default::default()
            }],
        };
        let (state, sink) = test_state();

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(snappy(&req.encode_to_vec())))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert!(records.len() == 1);
        let SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms,
        } = records[0].payload
        else {
            panic!("expected float payload");
        };
        check!(timestamp_ms == 7);
        check!((value - 3.0).abs() < f64::EPSILON);
        check!(start_timestamp_ms == Some(5));
    }

    #[tokio::test]
    async fn push_v1_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn push_v2_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn oversized_exemplar_labels_are_rejected() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_label_value(
                        &"x".repeat(129),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn oversized_label_names_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_label_name_len: bytes(7),
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn runtime_overrides_apply_label_limits_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r#"
overrides:
  tenant-tight:
    max_label_value_length: "2B"
"#,
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let tight_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-tight")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(tight_response.status() == StatusCode::BAD_REQUEST);
        check!(loose_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn oversized_sample_sets_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_samples(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn validation_counts_exemplars_toward_samples_per_series_limit() {
        let mut labels = Labels::new();
        labels.insert("__name__", "http_requests_total");
        let mut exemplar_labels = Labels::new();
        exemplar_labels.insert("trace_id", "abc");
        let series = [DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: Vec::new(),
            exemplars: vec![
                DecodedExemplar {
                    labels: exemplar_labels.clone(),
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
            metadata: None,
        }];

        let err = validate(
            &series,
            &TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            },
        )
        .unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("samples per series 2 exceeds limit 1"));
    }

    #[test]
    fn validation_rejects_invalid_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid label name"));
        }
    }

    #[test]
    fn validation_rejects_invalid_exemplar_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            let mut exemplar_labels = Labels::new();
            exemplar_labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: vec![DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 1000,
                    value: 1.0,
                }],
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid exemplar label name"));
        }
    }

    #[tokio::test]
    async fn ingestion_rate_limit_returns_429_without_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate: per_sec(1),
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(first_response.status() == StatusCode::NO_CONTENT);
        check!(second_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn concurrent_pushes_cannot_overshoot_active_series_limit() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r"
defaults:
  max_global_series_per_user: 1
",
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        // Two distinct series pushed concurrently; the check-and-insert is a
        // single locked critical section, so exactly one is admitted and the
        // other is rejected rather than both passing the pre-insert count.
        let request = |name: &str| {
            app.clone().oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", name)])))
                    .unwrap(),
            )
        };
        let (first, second) = tokio::join!(request("series_a"), request("series_b"));
        let statuses = [first.unwrap().status(), second.unwrap().status()];

        let admitted = statuses
            .iter()
            .filter(|status| **status == StatusCode::NO_CONTENT)
            .count();
        let rejected = statuses
            .iter()
            .filter(|status| **status == StatusCode::BAD_REQUEST)
            .count();
        check!(admitted == 1);
        check!(rejected == 1);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn ingestion_rate_limit_counts_exemplar_only_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate: per_sec(1),
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let exemplar_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let sample_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_001)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(exemplar_response.status() == StatusCode::NO_CONTENT);
        check!(sample_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn too_old_samples_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window: millis(100),
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let within_window_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(within_window_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn runtime_overrides_apply_out_of_order_window_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r#"
defaults:
  out_of_order_time_window: "0ms"
overrides:
  tenant-loose:
    out_of_order_time_window: "100ms"
"#,
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let overridden_window_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(overridden_window_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn too_old_exemplar_only_series_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window: millis(100),
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_rejects_invalid_tenant_with_400() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "bad tenant")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_requires_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_rejects_non_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "gzip")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn unsupported_content_type_is_415() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(vec![1, 2, 3]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_metrics_returns_200_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        check!(sample.tenant == "tenant-a");
        check!(
            sample.labels
                == vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ]
        );
        assert!(matches!(
            sample.payload,
            SamplePayload::Float {
                timestamp_ms: 1,
                value: 0.5,
                ..
            }
        ));
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "system_cpu_utilization".to_string(),
                    metric_type: "gauge".to_string(),
                    help: "CPU utilization ratio.".to_string(),
                    unit: "1".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_appends() {
        let (state, sink) = test_state();
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let service = otlp_metrics_service(state);
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = service.export(request).await.expect("otlp grpc export");

        let records = sink.records();
        assert!(response.into_inner().partial_success.is_none());
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        assert!(sample.tenant == "tenant-a");
        assert!(
            sample.labels
                == vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ]
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_round_trips_over_bound_server() {
        let (state, sink) = test_state();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("serve distributor");
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let mut client = MetricsServiceClient::connect(format!("http://{bound}"))
            .await
            .expect("connect otlp grpc client");
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = client.export(request).await.expect("otlp grpc export");
        let _ = shutdown_tx.send(());

        let records = sink.records();
        check!(response.into_inner().partial_success.is_none());
        check!(records.len() == 2);
        check!(records.iter().any(|record| record.tenant == "tenant-a"));
    }

    #[tokio::test]
    async fn otlp_metrics_rejects_non_protobuf_content_type() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_delta_sum_accumulates_across_pushes() {
        let (state, sink) = test_state();
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        7.0,
                        2_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        5.0,
                        3_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(first_response.status() == StatusCode::OK);
        assert!(second_response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert!(float_records.len() == 2);
        assert!(matches!(
            float_records[0].payload,
            SamplePayload::Float {
                timestamp_ms: 2,
                value: 7.0,
                ..
            }
        ));
        assert!(matches!(
            float_records[1].payload,
            SamplePayload::Float {
                timestamp_ms: 3,
                value: 12.0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn otlp_resource_attributes_append_target_info() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_resource_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert!(float_records.len() == 2);
        let target = records
            .iter()
            .find(|record| {
                matches!(record.payload, SamplePayload::Float { .. })
                    && record
                        .labels
                        .iter()
                        .any(|(name, value)| name == "__name__" && value == "target_info")
            })
            .expect("target_info wal record");
        assert!(
            target.labels
                == vec![
                    ("__name__".to_string(), "target_info".to_string()),
                    ("service_name".to_string(), "checkout".to_string()),
                ]
        );
        assert!(matches!(
            target.payload,
            SamplePayload::Float {
                timestamp_ms: 1,
                value: 1.0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn non_elected_replica_returns_202_without_append() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r2"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::ACCEPTED);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn non_elected_v2_replica_returns_zero_written_headers() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_ha_replica("r2")))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == StatusCode::ACCEPTED);
        for header in [
            "X-Prometheus-Remote-Write-Samples-Written",
            "X-Prometheus-Remote-Write-Histograms-Written",
            "X-Prometheus-Remote-Write-Exemplars-Written",
        ] {
            check!(
                response
                    .headers()
                    .get(header)
                    .and_then(|value| value.to_str().ok())
                    == Some("0"),
                "header {header}",
            );
        }
        check!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persists_election_before_append() {
        let sink = Arc::new(RecordingSink::default());
        let election_sink = Arc::new(RecordingHaElectionSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_ha_election_sink(election_sink.clone()),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
        let elections = election_sink.elections();
        assert!(elections.len() == 1);
        check!(elections[0].tenant == "tenant-a");
        check!(elections[0].cluster == "c1");
        check!(elections[0].replica == "r1");
        check!(elections[0].lease_timestamp_ms > 0);
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persistence_failure_prevents_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone())
                .with_ha_election_sink(Arc::new(FailingHaElectionSink)),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn ha_election_records_round_trip_with_compacted_key() {
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };

        let encoded = record.encode().unwrap();

        assert!(HaElectionRecord::decode(&encoded).unwrap() == record);
        assert!(ha_election_compaction_key(&record) == Bytes::from_static(b"tenant-a\0c1"));
    }

    #[test]
    fn replay_ha_election_records_applies_tracker_and_reports_commit_offsets() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let records = vec![
            HaElectionConsumerRecord {
                topic: "ignored".to_string(),
                partition: PartitionIndex(0),
                offset: Offset(10),
                value: Some(record.encode().unwrap()),
            },
            HaElectionConsumerRecord {
                topic: HA_TRACKER_TOPIC.to_string(),
                partition: PartitionIndex(2),
                offset: Offset(20),
                value: Some(record.encode().unwrap()),
            },
        ];

        let result = replay_ha_election_records(&tracker, HA_TRACKER_TOPIC, &records).unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 2,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(2),
                        offset: Offset(21),
                    }],
                }
        );
        assert!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    /// A poll that replays nothing must not commit. Committing on an empty
    /// batch would advance the group past records it never applied, and the
    /// elections they carry would be lost on the next restart.
    #[tokio::test]
    async fn poll_ha_election_consumer_once_does_not_commit_without_progress() {
        let tracker = HaTracker::default();

        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![]],
            commit_calls: 0,
        };
        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();
        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 0,
                    replayed_records: 0,
                    committed_offsets: vec![],
                }
        );
        assert!(consumer.commit_calls == 0, "an empty poll commits nothing");

        // Polled but not replayed: a record from another topic is seen and
        // applied to nothing. Committing here would advance this group's
        // offsets on the strength of someone else's records.
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![consumer_record(
                "some-other-topic",
                1,
                7,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };
        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();
        assert!(result.polled_records == 1, "the record was seen");
        assert!(result.replayed_records == 0, "but it was not ours to apply");
        assert!(
            consumer.commit_calls == 0,
            "a poll that applies nothing commits nothing"
        );
    }

    /// The election consumer loop polls until told to stop, accumulating
    /// what each poll saw. A caller watching the summary to decide when it
    /// has caught up depends on every field advancing on every poll.
    #[tokio::test]
    async fn the_ha_election_loop_accumulates_every_polls_result() {
        let tracker = HaTracker::default();
        let record = |cluster: &str| {
            HaElectionRecord {
                tenant: "tenant-a".to_string(),
                cluster: cluster.to_string(),
                replica: "r1".to_string(),
                lease_timestamp_ms: 42_000,
            }
            .encode()
            .unwrap()
        };

        // Two records, then one, then none, so a loop that reused a single
        // poll's result would not add up.
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![
                vec![
                    consumer_record(HA_TRACKER_TOPIC, 0, 1, Some(record("c1"))),
                    consumer_record(HA_TRACKER_TOPIC, 1, 5, Some(record("c2"))),
                ],
                vec![consumer_record(HA_TRACKER_TOPIC, 2, 9, Some(record("c3")))],
                vec![],
            ],
            commit_calls: 0,
        };

        let summary = run_ha_election_consumer_loop(
            &mut consumer,
            &tracker,
            HA_TRACKER_TOPIC,
            millis(1),
            |summary| summary.polls >= 3,
        )
        .await
        .unwrap();

        assert!(
            summary.polls == 3,
            "one count per poll, including the empty one"
        );
        assert!(summary.polled_records == 3, "2 + 1 + 0");
        assert!(summary.replayed_records == 3);
        assert!(
            summary.committed_offsets
                == vec![
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(2),
                    },
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(6),
                    },
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(2),
                        offset: Offset(10),
                    },
                ],
            "offsets from every poll, in order"
        );
        assert!(
            consumer.commit_calls == 2,
            "the empty poll committed nothing"
        );
    }

    /// The stop predicate is consulted after each poll, so a loop told to
    /// stop immediately still does exactly one poll's worth of work.
    #[tokio::test]
    async fn the_ha_election_loop_stops_after_the_poll_that_satisfies_it() {
        let tracker = HaTracker::default();
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![], vec![]],
            commit_calls: 0,
        };

        let summary = run_ha_election_consumer_loop(
            &mut consumer,
            &tracker,
            HA_TRACKER_TOPIC,
            millis(1),
            |_| true,
        )
        .await
        .unwrap();

        assert!(summary.polls == 1, "stopping at once still polls once");
        assert!(
            consumer.batches.len() == 1,
            "and consumes exactly one batch"
        );
    }

    #[tokio::test]
    async fn poll_ha_election_consumer_once_replays_records_and_commits_on_progress() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![consumer_record(
                HA_TRACKER_TOPIC,
                1,
                7,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 1,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(8),
                    }],
                }
        );
        check!(consumer.commit_calls == 1);
        check!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn wal_records_from_series_fans_out_float_samples() {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let series = [DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(10, 1.0), DecodedSample::new(20, 2.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }];

        let records = wal_records_from_series("tenant-a", &series);

        assert!(records.len() == 2);
        check!(records[0].tenant == "tenant-a");
        check!(records[0].labels == records[1].labels);
        assert!(matches!(
            records[0].payload,
            SamplePayload::Float {
                timestamp_ms: 10,
                value: 1.0,
                ..
            }
        ));
        assert!(matches!(
            records[1].payload,
            SamplePayload::Float {
                timestamp_ms: 20,
                value: 2.0,
                ..
            }
        ));
    }
}

// === split-modules: generated submodules ===
mod append_clock_readings;
mod append_decoded_series;
mod append_wal_records;
mod clock_identity_labels;
mod clock_projection;
mod clock_reading_metric;
mod clock_series;
mod clock_state_series;
mod clock_wal_records;
mod clocks_push;
mod clocks_push_inner;
mod consumer;
mod decoded_sample_count;
mod decoded_series;
mod default_distributor_max_decompressed;
mod distributor_state;
mod enforce_and_record_active_series;
mod enforce_ingest_limits;
mod enforce_ingestion_rate;
mod enforce_label_limits;
mod enforce_out_of_order_window;
mod ha_election_compaction_key;
mod ha_election_consumer_commit;
mod ha_election_consumer_error;
mod ha_election_consumer_loop_summary;
mod ha_election_consumer_poll;
mod ha_election_consumer_record;
mod ha_election_partition_offset;
mod ha_election_replay_error;
mod ha_election_replay_result;
mod ha_election_sink;
mod header_list_includes;
mod indicator;
mod ingest_span;
mod ingest_stamp;
mod insert_written_header;
mod is_valid_label_name;
mod kafka_ha_election_sink;
mod kafka_sink;
mod keyed_producer_record;
mod label_pairs;
mod max_exemplar_label_codepoints;
mod otlp_grpc_export_inner;
mod otlp_metrics_service;
mod otlp_metrics_service_server;
mod otlp_push;
mod otlp_push_inner;
mod poll_ha_election_consumer_once;
mod produce_error;
mod projected_labels;
mod push;
mod push_error;
mod push_inner;
mod push_success;
mod record_ingest_outcome;
mod replay_ha_election_records;
mod require_otlp_protobuf_content_type;
mod require_snappy_encoding;
mod router;
mod run_ha_election_consumer_loop;
mod sample_timestamp_bounds;
mod serve;
mod status_from_http_status;
mod status_from_push_error;
mod tenant_for_span;
mod tenant_from_headers;
mod tenant_from_metadata;
mod tenant_limits;
mod tenant_limits_to_limits;
mod validate;
mod validate_exemplar_labels;
mod validate_request_tenant;
mod wal_producer_record;
mod wal_records_from_series;
mod wal_sink;
mod widen;
mod written_counts_response;

use append_clock_readings::append_clock_readings;
use append_decoded_series::append_decoded_series;
use append_wal_records::append_wal_records;
use clock_identity_labels::clock_identity_labels;
use clock_projection::clock_projection;
pub use clock_reading_metric::CLOCK_READING_METRIC;
pub use clock_series::clock_series;
use clock_state_series::clock_state_series;
pub use clock_wal_records::clock_wal_records;
use clocks_push::clocks_push;
use clocks_push_inner::clocks_push_inner;
use decoded_sample_count::decoded_sample_count;
use decoded_series::decoded_series;
pub use default_distributor_max_decompressed::DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED;
pub use distributor_state::DistributorState;
use enforce_and_record_active_series::enforce_and_record_active_series;
use enforce_ingest_limits::enforce_ingest_limits;
use enforce_ingestion_rate::enforce_ingestion_rate;
use enforce_label_limits::enforce_label_limits;
use enforce_out_of_order_window::enforce_out_of_order_window;
pub use ha_election_compaction_key::ha_election_compaction_key;
pub use ha_election_consumer_commit::HaElectionConsumerCommit;
pub use ha_election_consumer_error::HaElectionConsumerError;
pub use ha_election_consumer_loop_summary::HaElectionConsumerLoopSummary;
pub use ha_election_consumer_poll::HaElectionConsumerPoll;
pub use ha_election_consumer_record::HaElectionConsumerRecord;
pub use ha_election_partition_offset::HaElectionPartitionOffset;
pub use ha_election_replay_error::HaElectionReplayError;
pub use ha_election_replay_result::HaElectionReplayResult;
pub use ha_election_sink::HaElectionSink;
use header_list_includes::header_list_includes;
use indicator::indicator;
use ingest_span::ingest_span;
use ingest_stamp::ingest_stamp;
use insert_written_header::insert_written_header;
use is_valid_label_name::is_valid_label_name;
pub use kafka_ha_election_sink::KafkaHaElectionSink;
pub use kafka_sink::KafkaSink;
use keyed_producer_record::keyed_producer_record;
use label_pairs::label_pairs;
use max_exemplar_label_codepoints::MAX_EXEMPLAR_LABEL_CODEPOINTS;
use otlp_grpc_export_inner::otlp_grpc_export_inner;
pub use otlp_metrics_service::otlp_metrics_service;
pub use otlp_metrics_service::OtlpMetricsService;
pub use otlp_metrics_service_server::otlp_metrics_service_server;
use otlp_push::otlp_push;
use otlp_push_inner::otlp_push_inner;
pub use poll_ha_election_consumer_once::poll_ha_election_consumer_once;
pub use produce_error::ProduceError;
use projected_labels::projected_labels;
use push::push;
use push_error::PushError;
use push_inner::push_inner;
use push_success::PushSuccess;
use record_ingest_outcome::record_ingest_outcome;
pub use replay_ha_election_records::replay_ha_election_records;
use require_otlp_protobuf_content_type::require_otlp_protobuf_content_type;
use require_snappy_encoding::require_snappy_encoding;
pub use router::router;
pub use run_ha_election_consumer_loop::run_ha_election_consumer_loop;
use sample_timestamp_bounds::sample_timestamp_bounds;
pub use serve::serve;
use status_from_http_status::status_from_http_status;
use status_from_push_error::status_from_push_error;
use tenant_for_span::tenant_for_span;
# [cfg_attr (test , mutants :: skip)] use tenant_from_headers::tenant_from_headers;
# [cfg_attr (test , mutants :: skip)] use tenant_from_metadata::tenant_from_metadata;
pub use tenant_limits::TenantLimits;
use tenant_limits_to_limits::tenant_limits_to_limits;
pub use validate::validate;
use validate_exemplar_labels::validate_exemplar_labels;
# [cfg_attr (test , mutants :: skip)] use validate_request_tenant::validate_request_tenant;
use wal_producer_record::wal_producer_record;
pub use wal_records_from_series::wal_records_from_series;
pub use wal_sink::WalSink;
use widen::widen;
use written_counts_response::written_counts_response;
