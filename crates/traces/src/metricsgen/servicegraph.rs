//! Service graph edge pairing processor.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use krabka_units::{Time, convert::TimeExt as _};
use num_traits::ToPrimitive as _;

use crate::metricsgen::{
    checkpoint::{CheckpointCodecError, encode_checkpoint_key, parse_checkpoint_key},
    config::MetricsGenConfig,
    contract::{SpanKind, SpanRecord, StatusCode},
    series::{HistogramBucket, Series, SeriesSample, sorted_labels},
};

/// Tempo service-graph connection classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionType {
    Unset,
    VirtualNode,
    MessagingSystem,
    Database,
}

impl ConnectionType {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::VirtualNode => "virtual_node",
            Self::MessagingSystem => "messaging_system",
            Self::Database => "database",
        }
    }
}

pub(crate) fn decode_checkpoint_value(mut buf: &[u8]) -> Result<Edge, CheckpointCodecError> {
    if buf.len() < 10 {
        return Err(CheckpointCodecError::Truncated);
    }
    let connection_type = match buf.get_u8() {
        0 => ConnectionType::Unset,
        1 => ConnectionType::VirtualNode,
        2 => ConnectionType::MessagingSystem,
        3 => ConnectionType::Database,
        _ => return Err(CheckpointCodecError::BadConnectionType),
    };
    let first_seen_ns = buf.get_i64();
    let failed = buf.get_u8() != 0;
    let client_service = get_optional_string(&mut buf)?;
    let server_service = get_optional_string(&mut buf)?;
    let client_latency_ns = get_optional_i64(&mut buf)?;
    let server_latency_ns = get_optional_i64(&mut buf)?;
    let mut multiplier = 1.0;
    let mut labels = Vec::new();
    if !buf.is_empty() {
        if buf.len() < 12 {
            return Err(CheckpointCodecError::Truncated);
        }
        multiplier = buf.get_f64();
        let count = buf.get_u32();
        for _ in 0..count {
            let name = get_optional_string(&mut buf)?.ok_or(CheckpointCodecError::Truncated)?;
            let value = get_optional_string(&mut buf)?.ok_or(CheckpointCodecError::Truncated)?;
            labels.push(krabka_traceql::MetricLabel::new(name, value));
        }
    }
    Ok(Edge {
        client_service,
        server_service,
        client_latency_ns,
        server_latency_ns,
        failed,
        connection_type,
        first_seen_ns,
        labels,
        multiplier,
    })
}

/// A half-edge until both client and server sides arrive.
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    pub client_service: Option<String>,
    pub server_service: Option<String>,
    pub client_latency_ns: Option<i64>,
    pub server_latency_ns: Option<i64>,
    pub failed: bool,
    pub connection_type: ConnectionType,
    pub first_seen_ns: i64,
    pub labels: Vec<krabka_traceql::MetricLabel>,
    pub multiplier: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EdgeAgg {
    pub(crate) requests: f64,
    pub(crate) failed: f64,
    pub(crate) client_seconds_sum: f64,
    pub(crate) client_seconds_count: f64,
    pub(crate) client_bucket_counts: Vec<f64>,
    pub(crate) server_seconds_sum: f64,
    pub(crate) server_seconds_count: f64,
    pub(crate) server_bucket_counts: Vec<f64>,
    pub(crate) messaging_seconds_sum: f64,
    pub(crate) messaging_seconds_count: f64,
    pub(crate) messaging_bucket_counts: Vec<f64>,
}

impl EdgeAgg {
    pub(crate) fn new(bucket_count: usize) -> Self {
        Self {
            client_bucket_counts: vec![0.0; bucket_count],
            server_bucket_counts: vec![0.0; bucket_count],
            messaging_bucket_counts: vec![0.0; bucket_count],
            ..Self::default()
        }
    }
}

pub(crate) fn get_optional_string(buf: &mut &[u8]) -> Result<Option<String>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    let value = String::from_utf8(super::read_length_prefixed_bytes(buf)?)
        .map_err(|_| CheckpointCodecError::Utf8)?;
    Ok(Some(value))
}

#[derive(Clone, Copy)]
pub(crate) struct HistogramSnapshot<'a> {
    pub(crate) sum: f64,
    pub(crate) count: f64,
    pub(crate) bucket_edges_ns: &'a [f64],
    pub(crate) bucket_counts: &'a [f64],
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LabelKey {
    pub(crate) client: String,
    pub(crate) server: String,
    pub(crate) connection_type: ConnectionType,
    pub(crate) dimensions: Vec<krabka_traceql::MetricLabel>,
}

/// Result of recording one span into the edge store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    Completed,
    Dropped,
    Ignored,
}

#[cfg(test)]
mod tests {

    /// `EdgeStore::complete` folds one finished edge into the aggregate for
    /// its client/server/kind triple. Each counter is checked after a run of
    /// edges rather than after one, since a counter that increments by the
    /// wrong amount, or from the wrong field, still moves.
    #[test]
    fn completing_edges_accumulates_per_triple() {
        let edge = |client: &str, server: &str, failed: bool, client_ns, server_ns| super::Edge {
            client_service: Some(client.to_string()),
            server_service: Some(server.to_string()),
            client_latency_ns: client_ns,
            server_latency_ns: server_ns,
            failed,
            connection_type: super::ConnectionType::Unset,
            first_seen_ns: 0,
            labels: Vec::new(),
            multiplier: 1.0,
        };
        // Every figure here is a whole number of requests, or a sum of whole
        // seconds, so each is exactly representable. The comparison carries a
        // tolerance because a bare `==` on a float is refused, not because any
        // of these is expected to drift.
        let is = |actual: f64, expected: f64| (actual - expected).abs() < f64::EPSILON;
        let key = |client: &str, server: &str| super::LabelKey {
            client: client.to_string(),
            server: server.to_string(),
            connection_type: super::ConnectionType::Unset,
            dimensions: Vec::new(),
        };

        let mut store = super::EdgeStore::new(&super::MetricsGenConfig::default());
        store.complete(&edge(
            "a",
            "b",
            false,
            Some(1_000_000_000),
            Some(2_000_000_000),
        ));
        store.complete(&edge("a", "b", true, Some(3_000_000_000), None));
        store.complete(&edge("c", "d", false, None, None));

        let agg = &store.aggregates[&key("a", "b")];
        check!(is(agg.requests, 2.0), "one per completed edge");
        check!(is(agg.failed, 1.0), "only the failed one counts");
        check!(
            is(agg.client_seconds_count, 2.0),
            "both carried a client latency"
        );
        check!(is(agg.client_seconds_sum, 4.0), "one second plus three");
        check!(
            is(agg.server_seconds_count, 1.0),
            "only one carried a server latency"
        );
        check!(is(agg.server_seconds_sum, 2.0));

        // A different triple aggregates separately rather than merging.
        let other = &store.aggregates[&key("c", "d")];
        check!(is(other.requests, 1.0));
        check!(is(other.failed, 0.0), "not failed");
        check!(
            is(other.client_seconds_count, 0.0),
            "no latency recorded at all"
        );
        check!(is(other.server_seconds_count, 0.0));
        check!(store.aggregates.len() == 2, "two triples, not one");

        // Messaging latency is off by default, so a messaging edge records
        // nothing under it even when it carries a latency.
        let mut store = super::EdgeStore::new(&super::MetricsGenConfig::default());
        let mut messaging = edge("a", "b", false, Some(1_000_000_000), Some(2_000_000_000));
        messaging.connection_type = super::ConnectionType::MessagingSystem;
        store.complete(&messaging);
        let messaging_key = super::LabelKey {
            client: "a".to_string(),
            server: "b".to_string(),
            connection_type: super::ConnectionType::MessagingSystem,
            dimensions: Vec::new(),
        };
        let agg = &store.aggregates[&messaging_key];
        check!(is(agg.messaging_seconds_count, 0.0), "disabled by default");

        // With it enabled, the server latency is preferred over the client's.
        let cfg = super::MetricsGenConfig {
            enable_messaging_system_latency: true,
            ..super::MetricsGenConfig::default()
        };
        let mut store = super::EdgeStore::new(&cfg);
        store.complete(&messaging);
        let agg = &store.aggregates[&messaging_key];
        check!(is(agg.messaging_seconds_count, 1.0));
        check!(
            is(agg.messaging_seconds_sum, 2.0),
            "the server latency, not the client's"
        );
    }

    /// An edge survives a round trip through the checkpoint codec.
    ///
    /// Every field holds a value distinct from every other, so a decoder that
    /// reads them in the wrong order is caught: the two service names and the
    /// two latencies are adjacent pairs of the same shape, and swapping either
    /// pair is invisible when both sides carry the same value.
    #[test]
    fn an_edge_round_trips_through_the_checkpoint_codec() {
        let edge = super::Edge {
            client_service: Some("client-svc".to_string()),
            server_service: Some("server-svc".to_string()),
            client_latency_ns: Some(11),
            server_latency_ns: Some(22),
            failed: true,
            connection_type: super::ConnectionType::Database,
            first_seen_ns: 1_234_567,
            labels: vec![krabka_traceql::MetricLabel::new("cluster", "prod")],
            multiplier: 2.0,
        };
        let decoded = super::decode_checkpoint_value(&super::encode_checkpoint_value(&edge))
            .expect("round trip");
        check!(decoded == edge);

        // Absent optionals stay absent rather than becoming defaults.
        let sparse = super::Edge {
            client_service: None,
            server_service: None,
            client_latency_ns: None,
            server_latency_ns: None,
            failed: false,
            connection_type: super::ConnectionType::Unset,
            first_seen_ns: 0,
            labels: Vec::new(),
            multiplier: 1.0,
        };
        let decoded = super::decode_checkpoint_value(&super::encode_checkpoint_value(&sparse))
            .expect("round trip");
        check!(decoded == sparse);

        // One side present and the other absent, which is what a swapped pair
        // turns into rather than a difference in value.
        let half = super::Edge {
            client_service: Some("only-client".to_string()),
            server_service: None,
            client_latency_ns: None,
            server_latency_ns: Some(99),
            ..edge.clone()
        };
        let decoded = super::decode_checkpoint_value(&super::encode_checkpoint_value(&half))
            .expect("round trip");
        check!(decoded == half);

        // Every connection type survives, and each is distinguishable.
        for connection_type in [
            super::ConnectionType::Unset,
            super::ConnectionType::VirtualNode,
            super::ConnectionType::MessagingSystem,
            super::ConnectionType::Database,
        ] {
            let one = super::Edge {
                connection_type,
                ..edge.clone()
            };
            let decoded = super::decode_checkpoint_value(&super::encode_checkpoint_value(&one))
                .expect("round trip");
            check!(
                decoded.connection_type == connection_type,
                "{connection_type:?}"
            );
        }

        // A negative first-seen timestamp is a value, not a sentinel.
        let past = super::Edge {
            first_seen_ns: -5,
            ..edge.clone()
        };
        let decoded = super::decode_checkpoint_value(&super::encode_checkpoint_value(&past))
            .expect("round trip");
        check!(decoded.first_seen_ns == -5);
    }

    /// The checkpoint decoder rejects what it cannot read rather than
    /// returning a partly-filled edge.
    #[test]
    fn a_malformed_checkpoint_value_is_rejected() {
        // Shorter than the fixed header.
        check!(super::decode_checkpoint_value(&[]).is_err());
        check!(
            super::decode_checkpoint_value(&[0; 9]).is_err(),
            "one byte short of ten"
        );

        // A connection type outside the four defined ones.
        let mut bytes = vec![4_u8];
        bytes.extend_from_slice(&0_i64.to_be_bytes());
        bytes.push(0);
        check!(
            matches!(
                super::decode_checkpoint_value(&bytes),
                Err(super::CheckpointCodecError::BadConnectionType)
            ),
            "an unknown connection type names itself"
        );

        // A well-formed header followed by a truncated optional field.
        let mut bytes = vec![0_u8];
        bytes.extend_from_slice(&0_i64.to_be_bytes());
        bytes.push(0);
        bytes.push(1);
        check!(
            super::decode_checkpoint_value(&bytes).is_err(),
            "string length missing"
        );
    }

    /// The optional decoders read a presence byte, then the value, and leave
    /// the cursor exactly past what they consumed. Each case checks the
    /// remaining buffer as well as the value: a decoder that returns the right
    /// answer but misplaces the cursor corrupts every field after it, and the
    /// value alone cannot see that.
    #[test]
    fn optional_fields_decode_and_leave_the_cursor_past_them() {
        // Absent: one presence byte consumed, nothing else.
        let mut buf = &[0_u8, 0xff][..];
        check!(super::get_optional_i64(&mut buf).expect("absent") == None);
        check!(buf == &[0xff], "only the presence byte is consumed");

        // Present: presence byte plus eight big-endian bytes.
        let mut buf = &[1, 0, 0, 0, 0, 0, 0, 0, 7, 0xff][..];
        check!(super::get_optional_i64(&mut buf).expect("present") == Some(7));
        check!(buf == &[0xff], "and the value after it");

        // Negative values survive the round trip as two's complement.
        let mut buf = &[1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff][..];
        check!(super::get_optional_i64(&mut buf).expect("present") == Some(-1));

        // Any non-zero presence byte means present, not just 1.
        let mut buf = &[2, 0, 0, 0, 0, 0, 0, 0, 9][..];
        check!(super::get_optional_i64(&mut buf).expect("present") == Some(9));

        // Truncation is an error rather than a short read.
        let mut buf = &[][..];
        check!(
            super::get_optional_i64(&mut buf).is_err(),
            "no presence byte"
        );
        let mut buf = &[1, 0, 0][..];
        check!(
            super::get_optional_i64(&mut buf).is_err(),
            "value cut short"
        );
        let mut buf = &[1, 0, 0, 0, 0, 0, 0, 0][..];
        check!(
            super::get_optional_i64(&mut buf).is_err(),
            "one byte short of eight"
        );

        // Strings carry a four-byte length ahead of their bytes.
        let mut buf = &[1, 0, 0, 0, 2, b'h', b'i', 0xff][..];
        check!(super::get_optional_string(&mut buf).expect("present") == Some("hi".to_string()));
        check!(
            buf == &[0xff],
            "cursor past the string, not past the buffer"
        );

        let mut buf = &[0, 0xff][..];
        check!(super::get_optional_string(&mut buf).expect("absent") == None);
        check!(buf == &[0xff]);

        // An empty string is present and zero-length, which is not absent.
        let mut buf = &[1, 0, 0, 0, 0, 0xff][..];
        check!(super::get_optional_string(&mut buf).expect("present") == Some(String::new()));
        check!(buf == &[0xff]);

        let mut buf = &[1, 0, 0][..];
        check!(
            super::get_optional_string(&mut buf).is_err(),
            "length cut short"
        );
        // Exactly one byte short of a four-byte length: the bound has to be
        // `< 4`, and `< 3` would read past the end.
        let mut buf = &[1, 0, 0, 0][..];
        check!(
            super::get_optional_string(&mut buf).is_err(),
            "three bytes of length"
        );
        // A string that exactly fills the buffer is complete, not truncated,
        // which is the case that separates `< len` from `< len + 1`.
        let mut buf = &[1, 0, 0, 0, 2, b'h', b'i'][..];
        check!(
            super::get_optional_string(&mut buf).expect("complete") == Some("hi".to_string()),
            "a string may end the buffer"
        );
        check!(buf.is_empty(), "and leave nothing behind");
        let mut buf = &[1, 0, 0, 0, 9, b'h'][..];
        check!(
            super::get_optional_string(&mut buf).is_err(),
            "declared longer than remains"
        );
        let mut buf = &[1, 0, 0, 0, 1, 0xff][..];
        check!(
            super::get_optional_string(&mut buf).is_err(),
            "not valid utf-8"
        );
    }
    use assert2::check;
    use krabka_units::secs;

    use super::*;
    use crate::metricsgen::{
        SpanAttribute,
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
        series::{Series, SeriesSample},
    };

    /// One span of a test call edge. The defaults are a root client span,
    /// status OK, that lasted one nanosecond.
    struct EdgeSpan<'a> {
        service: &'a str,
        span_id: [u8; 8],
        parent_span_id: [u8; 8],
        kind: SpanKind,
        status: StatusCode,
        duration_ns: i64,
    }

    impl Default for EdgeSpan<'_> {
        fn default() -> Self {
            Self {
                service: "service",
                span_id: [0x1; 8],
                parent_span_id: [0; 8],
                kind: SpanKind::Client,
                status: StatusCode::Ok,
                duration_ns: 1,
            }
        }
    }

    impl EdgeSpan<'_> {
        fn record(self) -> SpanRecord {
            SpanRecord {
                tenant: "t".into(),
                trace_id: [0x11; 16],
                span_id: self.span_id,
                parent_span_id: self.parent_span_id,
                name: "op".into(),
                kind: self.kind,
                duration_ns: self.duration_ns,
                status: self.status,
                service_name: self.service.into(),
                ..SpanRecord::default()
            }
        }
    }

    fn counter(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::Counter(c) => c,
                _ => panic!("{name} not a counter"),
            })
    }

    fn labels_for<'a>(series: &'a [Series], name: &str) -> &'a [krabka_traceql::MetricLabel] {
        &series.iter().find(|s| s.name == name).unwrap().labels
    }

    fn histogram_sum(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::ClassicHistogram { sum, .. } => sum,
                _ => panic!("{name} not a histogram"),
            })
    }

    fn histogram_count(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::ClassicHistogram { count, .. } => count,
                _ => panic!("{name} not a histogram"),
            })
    }

    fn histogram_bucket_value(series: &[Series], name: &str, le: f64) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match &s.sample {
                SeriesSample::ClassicHistogram { buckets, .. } => buckets
                    .iter()
                    .find(|bucket| (bucket.upper_bound - le).abs() < 1e-9)
                    .map_or(0.0, |bucket| bucket.cumulative_count),
                _ => panic!("{name} not a histogram"),
            })
    }

    /// An edge completes only once BOTH sides have been seen. Every other
    /// test pairs a client with a server, where the edge is two-sided by the
    /// time the update path runs and `&&` and `||` agree; and the creating
    /// span never reaches that test at all. A second client span on the same
    /// edge is the case that separates them.
    #[test]
    fn an_edge_needs_both_sides_before_it_completes() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            duration_ns: 10_000_000,
            ..EdgeSpan::default()
        }
        .record();
        // Same edge, same side: a retry of the same call.
        let retry = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            duration_ns: 12_000_000,
            ..EdgeSpan::default()
        }
        .record();
        let server = EdgeSpan {
            service: "backend",
            span_id: [0xB; 8],
            parent_span_id: [0xA; 8],
            kind: SpanKind::Server,
            duration_ns: 8_000_000,
            ..EdgeSpan::default()
        }
        .record();

        // The first span creates the edge. The second takes the update path
        // with the server side still missing, and must not complete it.
        assert2::check!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::check!(
            store.record_span(&retry, 1) == RecordOutcome::Recorded,
            "an edge with only a client side is still incomplete"
        );

        // Only the other side finishes it.
        assert2::check!(store.record_span(&server, 2) == RecordOutcome::Completed);
    }

    // Pair one 10ms `frontend` client span with its 8ms `backend` server
    // span under the default config, and drain the edge.
    /// One `frontend` client span calling one `backend` server span.
    #[derive(Clone, Copy)]
    struct FrontendBackendEdge {
        client_duration_ns: i64,
        server_duration_ns: i64,
        server_status: StatusCode,
    }

    impl Default for FrontendBackendEdge {
        fn default() -> Self {
            Self {
                client_duration_ns: 10_000_000,
                server_duration_ns: 8_000_000,
                server_status: StatusCode::Ok,
            }
        }
    }

    /// Records the client then the server half of `edge`, asserting they pair,
    /// and drains the store.
    fn drain_edge(edge: FrontendBackendEdge) -> Vec<Series> {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            duration_ns: edge.client_duration_ns,
            ..EdgeSpan::default()
        }
        .record();
        let server = EdgeSpan {
            service: "backend",
            span_id: [0xB; 8],
            parent_span_id: [0xA; 8],
            kind: SpanKind::Server,
            status: edge.server_status,
            duration_ns: edge.server_duration_ns,
        }
        .record();

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        store.drain(1_000)
    }

    #[test]
    fn pairs_client_then_server_into_one_request() {
        let out = drain_edge(FrontendBackendEdge::default());
        assert2::assert!((counter(&out, "traces_service_graph_request_total") - 1.0).abs() < 1e-9);
        assert2::assert!(counter(&out, "traces_service_graph_request_failed_total").abs() < 1e-9);

        let req = out
            .iter()
            .find(|s| s.name == "traces_service_graph_request_total")
            .unwrap();
        assert2::assert!(
            req.labels
                == [
                    krabka_traceql::MetricLabel::new("client", "frontend".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "unset".to_string()),
                    krabka_traceql::MetricLabel::new("server", "backend".to_string()),
                ]
        );
        check!(
            (histogram_sum(&out, "traces_service_graph_request_client_seconds") - 0.010).abs()
                < 1e-9
        );
        check!(
            (histogram_sum(&out, "traces_service_graph_request_server_seconds") - 0.008).abs()
                < 1e-9
        );
    }

    #[test]
    fn tenant_processor_config_controls_service_graph_labels_and_weight() {
        let mut cfg = MetricsGenConfig::default();
        cfg.processor.service_graphs.dimensions = vec!["http.method".into()];
        cfg.processor.service_graphs.enable_client_server_prefix = true;
        cfg.processor.service_graphs.span_multiplier_key = Some("sample.weight".into());
        let mut store = EdgeStore::new(&cfg);
        let mut client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            duration_ns: 10_000_000,
            ..EdgeSpan::default()
        }
        .record();
        client.attributes = vec![
            SpanAttribute::new("http.method", "GET"),
            SpanAttribute::new("sample.weight", "3"),
        ];
        let mut server = EdgeSpan {
            service: "backend",
            span_id: [0xB; 8],
            parent_span_id: [0xA; 8],
            kind: SpanKind::Server,
            duration_ns: 8_000_000,
            ..EdgeSpan::default()
        }
        .record();
        server
            .attributes
            .push(SpanAttribute::new("http.method", "POST"));

        check!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        check!(store.record_span(&server, 1) == RecordOutcome::Completed);
        let out = store.drain(1_000);
        check!((counter(&out, "traces_service_graph_request_total") - 3.0).abs() < f64::EPSILON);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        check!(labels.contains(&krabka_traceql::MetricLabel::new(
            "client_http_method",
            "GET"
        )));
        check!(labels.contains(&krabka_traceql::MetricLabel::new(
            "server_http_method",
            "POST"
        )));
        check!(
            (histogram_count(&out, "traces_service_graph_request_client_seconds") - 3.0).abs()
                < f64::EPSILON
        );
    }

    #[yare::parameterized(
        client_first = { "traces_service_graph_request_client_seconds", 0.008, 0.0 },
        client_second = { "traces_service_graph_request_client_seconds", 0.016, 1.0 },
        server_first = { "traces_service_graph_request_server_seconds", 0.008, 1.0 },
    )]
    fn request_latency_histograms_include_configured_buckets(name: &str, le: f64, want: f64) {
        let out = drain_edge(FrontendBackendEdge::default());
        check!(
            (histogram_bucket_value(&out, name, le) - want).abs() < 1e-9,
            "case {name} le={le}"
        );
    }

    #[test]
    fn unset_connection_type_is_labeled_explicitly() {
        let out = drain_edge(FrontendBackendEdge {
            client_duration_ns: 1,
            server_duration_ns: 1,
            server_status: StatusCode::Ok,
        });
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert2::assert!(
            labels
                .iter()
                .any(|label| label.key == "connection_type" && label.value == "unset")
        );
    }

    #[test]
    fn failed_when_either_side_errors() {
        let out = drain_edge(FrontendBackendEdge {
            client_duration_ns: 1,
            server_duration_ns: 1,
            server_status: StatusCode::Error,
        });
        assert2::assert!(
            (counter(&out, "traces_service_graph_request_failed_total") - 1.0).abs() < 1e-9
        );
    }

    // A store with a ten-second edge TTL that holds one `frontend` client span
    // whose server half never arrives.
    fn store_with_unpaired_frontend_client() -> EdgeStore {
        let cfg = MetricsGenConfig {
            edge_ttl: secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            ..EdgeSpan::default()
        }
        .record();
        check!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        store
    }

    #[test]
    fn unpaired_half_edge_expires_after_ttl() {
        let mut store = store_with_unpaired_frontend_client();

        check!(store.expire(5_000_000_000) == 0);
        check!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        assert2::assert!(
            (counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9
        );
    }

    #[test]
    fn unpaired_client_span_keeps_service_graph_labels() {
        let mut store = store_with_unpaired_frontend_client();
        assert2::assert!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert2::assert!(
            labels
                == [
                    krabka_traceql::MetricLabel::new("client", "frontend".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "unset".to_string()),
                    krabka_traceql::MetricLabel::new("server", String::new()),
                ]
        );
    }

    #[test]
    fn store_full_drops_new_spans() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let a = EdgeSpan {
            service: "s1",
            ..EdgeSpan::default()
        }
        .record();
        let b = EdgeSpan {
            service: "s2",
            span_id: [0x2; 8],
            ..EdgeSpan::default()
        }
        .record();

        assert2::assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);

        let out = store.drain(1_000);
        assert2::assert!(
            (counter(&out, "traces_service_graph_dropped_spans_total") - 1.0).abs() < 1e-9
        );
    }

    #[test]
    fn expired_half_edges_do_not_consume_store_capacity() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            edge_ttl: secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let stale = EdgeSpan {
            service: "stale-client",
            ..EdgeSpan::default()
        }
        .record();
        let fresh = EdgeSpan {
            service: "fresh-client",
            span_id: [0x2; 8],
            ..EdgeSpan::default()
        }
        .record();

        assert2::assert!(store.record_span(&stale, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&fresh, 10_000_000_000) == RecordOutcome::Recorded);

        let out = store.drain(1_000);
        assert2::assert!(
            (counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9
        );
        assert2::assert!(counter(&out, "traces_service_graph_dropped_spans_total").abs() < 1e-9);
    }

    #[test]
    fn dropped_client_span_keeps_service_graph_labels() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let a = EdgeSpan {
            service: "s1",
            ..EdgeSpan::default()
        }
        .record();
        let mut b = EdgeSpan {
            service: "database",
            span_id: [0x2; 8],
            ..EdgeSpan::default()
        }
        .record();
        b.attributes
            .push(SpanAttribute::new("db.system", "postgresql"));

        assert2::assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_dropped_spans_total");
        assert2::assert!(
            labels
                == [
                    krabka_traceql::MetricLabel::new("client", "database".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "database".to_string()),
                    krabka_traceql::MetricLabel::new("server", String::new()),
                ]
        );
    }

    #[test]
    fn non_client_server_spans_ignored() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let internal = EdgeSpan {
            service: "s",
            kind: SpanKind::Internal,
            ..EdgeSpan::default()
        }
        .record();

        assert2::assert!(store.record_span(&internal, 0) == RecordOutcome::Ignored);
    }

    #[test]
    fn database_connection_type_from_db_system_attr() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            ..EdgeSpan::default()
        }
        .record();
        client
            .attributes
            .push(SpanAttribute::new("db.system", "postgresql"));

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.expire(20_000_000_000) == 1);

        let out = store.drain(1_000);
        let unpaired = out
            .iter()
            .find(|s| s.name == "traces_service_graph_unpaired_spans_total")
            .unwrap();
        assert2::assert!(
            unpaired
                .labels
                .iter()
                .any(|label| label.key == "connection_type" && label.value == "database")
        );
    }

    #[test]
    fn virtual_node_uses_peer_service_as_server_label() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            ..EdgeSpan::default()
        }
        .record();
        client
            .attributes
            .push(SpanAttribute::new("peer.service", "db-proxy"));

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.expire(20_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert2::assert!(
            labels
                == [
                    krabka_traceql::MetricLabel::new("client", "frontend".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "virtual_node".to_string()),
                    krabka_traceql::MetricLabel::new("server", "db-proxy".to_string()),
                ]
        );
    }

    #[test]
    fn virtual_node_peer_backfill_is_order_independent_on_edge_update() {
        // An edge created first (no virtual-node signal), then updated by a span
        // carrying peer.service must end up labeled virtual_node WITH the peer
        // backfilled into the server label — the same result as if the
        // virtual-node span had arrived first (the create path).
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let server = EdgeSpan {
            service: "backend",
            span_id: [0xB; 8],
            parent_span_id: [0xA; 8],
            kind: SpanKind::Server,
            ..EdgeSpan::default()
        }
        .record();
        let mut client = EdgeSpan {
            service: "frontend",
            span_id: [0xA; 8],
            ..EdgeSpan::default()
        }
        .record();
        client
            .attributes
            .push(SpanAttribute::new("peer.service", "db-proxy"));

        // Server arrives first and creates the edge with no virtual-node signal.
        assert2::assert!(store.record_span(&server, 0) == RecordOutcome::Recorded);
        // Client update carries the virtual-node / peer.service signal.
        assert2::assert!(store.record_span(&client, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        // peer.service ("db-proxy") backfilled into the server label even though
        // the real server span ("backend") already set it on the create path.
        assert2::assert!(
            labels
                == [
                    krabka_traceql::MetricLabel::new("client", "frontend".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "virtual_node".to_string()),
                    krabka_traceql::MetricLabel::new("server", "db-proxy".to_string()),
                ]
        );
    }

    #[test]
    fn messaging_producer_consumer_pair_emits_service_graph_edge() {
        let cfg = MetricsGenConfig {
            enable_messaging_system_latency: true,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let mut producer = EdgeSpan {
            service: "publisher",
            span_id: [0xA; 8],
            kind: SpanKind::Producer,
            duration_ns: 7_000_000,
            ..EdgeSpan::default()
        }
        .record();
        producer
            .attributes
            .push(SpanAttribute::new("messaging.system", "kafka"));
        let mut consumer = EdgeSpan {
            service: "worker",
            span_id: [0xB; 8],
            parent_span_id: [0xA; 8],
            kind: SpanKind::Consumer,
            duration_ns: 5_000_000,
            ..EdgeSpan::default()
        }
        .record();
        consumer
            .attributes
            .push(SpanAttribute::new("messaging.system", "kafka"));

        assert2::assert!(store.record_span(&producer, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&consumer, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert2::assert!(
            labels
                == [
                    krabka_traceql::MetricLabel::new("client", "publisher".to_string()),
                    krabka_traceql::MetricLabel::new("connection_type", "messaging_system"),
                    krabka_traceql::MetricLabel::new("server", "worker".to_string()),
                ]
        );
        check!(
            (histogram_sum(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 0.005)
                .abs()
                < 1e-9
        );
        check!(
            (histogram_count(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 1.0)
                .abs()
                < 1e-9
        );
    }
}
mod edge_key;
mod edge_store;
use edge_key::{EdgeKey, edge_key};
pub use edge_store::EdgeStore;

pub(crate) const NS_PER_SEC: f64 = 1_000_000_000.0;

pub(crate) fn attr_value<'a>(span: &'a SpanRecord, name: &str) -> Option<&'a str> {
    span.attributes.iter().find_map(|attribute| {
        (attribute.key == name && !attribute.value.is_empty()).then_some(attribute.value.as_str())
    })
}

pub(crate) fn edge_side(kind: SpanKind) -> Option<bool> {
    match kind {
        SpanKind::Client | SpanKind::Producer => Some(true),
        SpanKind::Server | SpanKind::Consumer => Some(false),
        SpanKind::Unspecified | SpanKind::Internal => None,
    }
}

pub(crate) fn fill_edge(edge: &mut Edge, span: &SpanRecord, is_client: bool, latency_ns: i64) {
    if is_client {
        edge.client_service = Some(span.service_name.clone());
        edge.client_latency_ns = Some(latency_ns);
    } else {
        edge.server_service = Some(span.service_name.clone());
        edge.server_latency_ns = Some(latency_ns);
    }
}

pub(crate) fn get_optional_i64(buf: &mut &[u8]) -> Result<Option<i64>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    if buf.len() < 8 {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(Some(buf.get_i64()))
}

pub(crate) fn get_presence(buf: &mut &[u8]) -> Result<bool, CheckpointCodecError> {
    if buf.is_empty() {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(buf.get_u8() != 0)
}

pub(crate) fn has_attr(span: &SpanRecord, name: &str) -> bool {
    span.attributes
        .iter()
        .any(|attribute| attribute.key == name)
}

pub(crate) fn label_key_for_edge(edge: &Edge) -> LabelKey {
    LabelKey {
        client: edge.client_service.clone().unwrap_or_default(),
        server: edge.server_service.clone().unwrap_or_default(),
        connection_type: edge.connection_type,
        dimensions: edge.labels.clone(),
    }
}

pub(crate) fn ns_to_seconds(ns: i64) -> f64 {
    ns.max(0).to_f64().unwrap_or(f64::MAX) / NS_PER_SEC
}

pub(crate) fn put_optional_i64(buf: &mut BytesMut, value: Option<i64>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            buf.put_i64(value);
        }
        None => buf.put_u8(0),
    }
}

pub(crate) fn service_graph_labels(key: LabelKey) -> Vec<krabka_traceql::MetricLabel> {
    let mut dimensions = key.dimensions;
    dimensions.extend([
        krabka_traceql::MetricLabel::new("client", key.client),
        krabka_traceql::MetricLabel::new("server", key.server),
        krabka_traceql::MetricLabel::new("connection_type", key.connection_type.as_label()),
    ]);
    sorted_labels(dimensions)
}

pub(crate) fn classify(span: &SpanRecord) -> ConnectionType {
    if has_attr(span, "db.system") {
        ConnectionType::Database
    } else if has_attr(span, "messaging.system") {
        ConnectionType::MessagingSystem
    } else if has_attr(span, "peer.service") {
        ConnectionType::VirtualNode
    } else {
        ConnectionType::Unset
    }
}

pub(crate) fn counter(
    name: &str,
    labels: &[krabka_traceql::MetricLabel],
    value: f64,
    timestamp_ms: i64,
) -> Series {
    Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::Counter(value),
        exemplars: Vec::new(),
        timestamp_ms,
    }
}

pub(crate) fn cumulative_buckets_seconds(
    bucket_edges_ns: &[f64],
    bucket_counts: &[f64],
) -> Vec<HistogramBucket> {
    let mut cumulative = 0.0;
    bucket_edges_ns
        .iter()
        .enumerate()
        .map(|indexed_edge| {
            let idx = indexed_edge.0;
            let edge_ns = indexed_edge.1;
            cumulative += bucket_counts.get(idx).copied().unwrap_or_default();
            HistogramBucket {
                upper_bound: *edge_ns / NS_PER_SEC,
                cumulative_count: cumulative,
            }
        })
        .collect()
}

pub(crate) fn encode_checkpoint_value(edge: &Edge) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(edge.connection_type as u8);
    buf.put_i64(edge.first_seen_ns);
    buf.put_u8(u8::from(edge.failed));
    put_optional_string(&mut buf, edge.client_service.as_deref());
    put_optional_string(&mut buf, edge.server_service.as_deref());
    put_optional_i64(&mut buf, edge.client_latency_ns);
    put_optional_i64(&mut buf, edge.server_latency_ns);
    buf.put_f64(edge.multiplier);
    buf.put_u32(edge.labels.len().try_into().unwrap_or(u32::MAX));
    for label in &edge.labels {
        put_optional_string(&mut buf, Some(&label.key));
        put_optional_string(&mut buf, Some(&label.value));
    }
    buf.to_vec()
}

pub(crate) fn fill_virtual_node(
    edge: &mut Edge,
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
    peer_attributes: &[String],
) {
    if connection_type != ConnectionType::VirtualNode {
        return;
    }
    let Some(peer) = peer_attributes
        .iter()
        .find_map(|attribute| attr_value(span, attribute))
    else {
        return;
    };
    if is_client {
        edge.server_service = Some(peer.to_string());
    } else {
        edge.client_service = Some(peer.to_string());
    }
}

pub(crate) fn label_key_for_span(
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
    labels: Vec<krabka_traceql::MetricLabel>,
) -> LabelKey {
    if is_client {
        LabelKey {
            client: span.service_name.clone(),
            server: String::new(),
            connection_type,
            dimensions: labels,
        }
    } else {
        LabelKey {
            client: String::new(),
            server: span.service_name.clone(),
            connection_type,
            dimensions: labels,
        }
    }
}

pub(crate) fn observe_latency(
    bucket_edges_ns: &[f64],
    bucket_counts: &mut [f64],
    ns: i64,
    multiplier: f64,
) {
    let value_ns = ns.max(0).to_f64().unwrap_or(f64::MAX);
    let idx = bucket_edges_ns
        .iter()
        .position(|edge| value_ns <= *edge)
        .unwrap_or(bucket_edges_ns.len());
    if let Some(count) = bucket_counts.get_mut(idx) {
        *count += multiplier;
    }
}

pub(crate) fn push_histogram(
    out: &mut Vec<Series>,
    name: &str,
    labels: &[krabka_traceql::MetricLabel],
    histogram: HistogramSnapshot<'_>,
    timestamp_ms: i64,
) {
    out.push(Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::ClassicHistogram {
            buckets: cumulative_buckets_seconds(histogram.bucket_edges_ns, histogram.bucket_counts),
            sum: histogram.sum,
            count: histogram.count,
        },
        exemplars: Vec::new(),
        timestamp_ms,
    });
}

pub(crate) fn put_optional_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            let len = u32::try_from(value.len()).expect("service name too long");
            buf.put_u32(len);
            buf.put_slice(value.as_bytes());
        }
        None => buf.put_u8(0),
    }
}
