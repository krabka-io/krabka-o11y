//! Service graph edge pairing processor.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use krabka_units::{Time, convert::TimeExt as _};
use num_traits::ToPrimitive as _;

use crate::metricsgen::{
    checkpoint::{CheckpointCodecError, encode_checkpoint_key, parse_checkpoint_key},
    config::MetricsGenConfig,
    contract::{SpanKind, SpanRecord, StatusCode},
    series::{Series, SeriesSample, sorted_labels},
};

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
        };
        // Every figure here is a whole number of requests, or a sum of whole
        // seconds, so each is exactly representable. The comparison carries a
        // tolerance because a bare `==` on a float is refused, not because any
        // of these is expected to drift.
        let is = |actual: f64, expected: f64| (actual - expected).abs() < f64::EPSILON;
        let key = |client: &str, server: &str| {
            (
                client.to_string(),
                server.to_string(),
                super::ConnectionType::Unset,
            )
        };

        let mut store = super::EdgeStore::new(&super::MetricsGenConfig::default());
        store.complete(edge(
            "a",
            "b",
            false,
            Some(1_000_000_000),
            Some(2_000_000_000),
        ));
        store.complete(edge("a", "b", true, Some(3_000_000_000), None));
        store.complete(edge("c", "d", false, None, None));

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
        store.complete(messaging.clone());
        let agg = &store.aggregates[&(
            "a".to_string(),
            "b".to_string(),
            super::ConnectionType::MessagingSystem,
        )];
        check!(is(agg.messaging_seconds_count, 0.0), "disabled by default");

        // With it enabled, the server latency is preferred over the client's.
        let cfg = super::MetricsGenConfig {
            enable_messaging_system_latency: true,
            ..super::MetricsGenConfig::default()
        };
        let mut store = super::EdgeStore::new(&cfg);
        store.complete(messaging);
        let agg = &store.aggregates[&(
            "a".to_string(),
            "b".to_string(),
            super::ConnectionType::MessagingSystem,
        )];
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
    use krabka_units::{ByteSize, convert::ByteSizeExt as _, secs};

    use super::*;
    use crate::metricsgen::{
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
        series::{Series, SeriesSample},
    };

    fn span(
        service: &str,
        span_id: [u8; 8],
        parent: [u8; 8],
        kind: SpanKind,
        status: StatusCode,
        dur_ns: i64,
    ) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0x11; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            size: ByteSize::from_bytes(0),
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

    fn labels_for<'a>(series: &'a [Series], name: &str) -> &'a [(String, String)] {
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
                    .find(|(bucket_le, _)| (*bucket_le - le).abs() < 1e-9)
                    .map_or(0.0, |(_, count)| *count),
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
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            10_000_000,
        );
        // Same edge, same side: a retry of the same call.
        let retry = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            12_000_000,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            8_000_000,
        );

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

    #[test]
    fn pairs_client_then_server_into_one_request() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            10_000_000,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            8_000_000,
        );

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        assert2::assert!((counter(&out, "traces_service_graph_request_total") - 1.0).abs() < 1e-9);
        assert2::assert!(counter(&out, "traces_service_graph_request_failed_total").abs() < 1e-9);

        let req = out
            .iter()
            .find(|s| s.name == "traces_service_graph_request_total")
            .unwrap();
        assert2::assert!(
            req.labels
                == [
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "unset".to_string()),
                    ("server".to_string(), "backend".to_string()),
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
    fn request_latency_histograms_include_configured_buckets() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            10_000_000,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            8_000_000,
        );

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        for (name, le, want) in [
            ("traces_service_graph_request_client_seconds", 0.008, 0.0),
            ("traces_service_graph_request_client_seconds", 0.016, 1.0),
            ("traces_service_graph_request_server_seconds", 0.008, 1.0),
        ] {
            check!(
                (histogram_bucket_value(&out, name, le) - want).abs() < 1e-9,
                "case {name} le={le}"
            );
        }
    }

    #[test]
    fn unset_connection_type_is_labeled_explicitly() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            1,
        );

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert2::assert!(
            labels
                .iter()
                .any(|(k, v)| k == "connection_type" && v == "unset")
        );
    }

    #[test]
    fn failed_when_either_side_errors() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Error,
            1,
        );

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        assert2::assert!(
            (counter(&out, "traces_service_graph_request_failed_total") - 1.0).abs() < 1e-9
        );
    }

    #[test]
    fn unpaired_half_edge_expires_after_ttl() {
        let cfg = MetricsGenConfig {
            edge_ttl: secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        check!(store.record_span(&client, 0) == RecordOutcome::Recorded);

        check!(store.expire(5_000_000_000) == 0);
        check!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        assert2::assert!(
            (counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9
        );
    }

    #[test]
    fn unpaired_client_span_keeps_service_graph_labels() {
        let cfg = MetricsGenConfig {
            edge_ttl: secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert2::assert!(
            labels
                == [
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "unset".to_string()),
                    ("server".to_string(), String::new()),
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
        let a = span("s1", [0x1; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let b = span("s2", [0x2; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);

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
        let stale = span(
            "stale-client",
            [0x1; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let fresh = span(
            "fresh-client",
            [0x2; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );

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
        let a = span("s1", [0x1; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let mut b = span(
            "database",
            [0x2; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        b.attributes.push(("db.system".into(), "postgresql".into()));

        assert2::assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_dropped_spans_total");
        assert2::assert!(
            labels
                == [
                    ("client".to_string(), "database".to_string()),
                    ("connection_type".to_string(), "database".to_string()),
                    ("server".to_string(), String::new()),
                ]
        );
    }

    #[test]
    fn non_client_server_spans_ignored() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let internal = span("s", [0x1; 8], [0; 8], SpanKind::Internal, StatusCode::Ok, 1);

        assert2::assert!(store.record_span(&internal, 0) == RecordOutcome::Ignored);
    }

    #[test]
    fn database_connection_type_from_db_system_attr() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        client
            .attributes
            .push(("db.system".into(), "postgresql".into()));

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
                .any(|(k, v)| k == "connection_type" && v == "database")
        );
    }

    #[test]
    fn virtual_node_uses_peer_service_as_server_label() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        client
            .attributes
            .push(("peer.service".into(), "db-proxy".into()));

        assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.expire(20_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert2::assert!(
            labels
                == [
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "virtual_node".to_string()),
                    ("server".to_string(), "db-proxy".to_string()),
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
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            1,
        );
        let mut client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        client
            .attributes
            .push(("peer.service".into(), "db-proxy".into()));

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
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "virtual_node".to_string()),
                    ("server".to_string(), "db-proxy".to_string()),
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
        let mut producer = span(
            "publisher",
            [0xA; 8],
            [0; 8],
            SpanKind::Producer,
            StatusCode::Ok,
            7_000_000,
        );
        producer
            .attributes
            .push(("messaging.system".into(), "kafka".into()));
        let mut consumer = span(
            "worker",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Consumer,
            StatusCode::Ok,
            5_000_000,
        );
        consumer
            .attributes
            .push(("messaging.system".into(), "kafka".into()));

        assert2::assert!(store.record_span(&producer, 0) == RecordOutcome::Recorded);
        assert2::assert!(store.record_span(&consumer, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert2::assert!(
            labels
                == [
                    ("client".to_string(), "publisher".to_string()),
                    (
                        "connection_type".to_string(),
                        "messaging_system".to_string(),
                    ),
                    ("server".to_string(), "worker".to_string()),
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

// === split-modules: generated submodules ===
mod attr_value;
mod classify;
mod connection_type;
mod counter;
mod cumulative_buckets_seconds;
mod decode_checkpoint_value;
mod edge;
mod edge_agg;
mod edge_key;
mod edge_side;
mod edge_store;
mod encode_checkpoint_value;
mod fill_edge;
mod fill_virtual_node;
mod get_optional_i64;
mod get_optional_string;
mod get_presence;
mod has_attr;
mod histogram_snapshot;
mod label_key;
mod label_key_for_edge;
mod label_key_for_span;
mod ns_per_sec;
mod ns_to_seconds;
mod observe_latency;
mod push_histogram;
mod put_optional_i64;
mod put_optional_string;
mod record_outcome;
mod service_graph_labels;

use attr_value::attr_value;
use classify::classify;
pub use connection_type::ConnectionType;
use counter::counter;
use cumulative_buckets_seconds::cumulative_buckets_seconds;
use decode_checkpoint_value::decode_checkpoint_value;
pub use edge::Edge;
use edge_agg::EdgeAgg;
use edge_key::{EdgeKey, edge_key};
use edge_side::edge_side;
pub use edge_store::EdgeStore;
use encode_checkpoint_value::encode_checkpoint_value;
use fill_edge::fill_edge;
use fill_virtual_node::fill_virtual_node;
use get_optional_i64::get_optional_i64;
use get_optional_string::get_optional_string;
use get_presence::get_presence;
use has_attr::has_attr;
use histogram_snapshot::HistogramSnapshot;
use label_key::LabelKey;
use label_key_for_edge::label_key_for_edge;
use label_key_for_span::label_key_for_span;
use ns_per_sec::NS_PER_SEC;
use ns_to_seconds::ns_to_seconds;
use observe_latency::observe_latency;
use push_histogram::push_histogram;
use put_optional_i64::put_optional_i64;
use put_optional_string::put_optional_string;
pub use record_outcome::RecordOutcome;
use service_graph_labels::service_graph_labels;
