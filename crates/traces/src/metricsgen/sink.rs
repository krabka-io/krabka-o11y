//! Span source and `remote_write` sink traits.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use krabka_client_consumer::{Consumer, ConsumerRecord};
use krabka_units::{ByteSize, Time, convert::ByteSizeExt as _, millis};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    metricsgen::{contract::SpanRecord, series::SeriesPayload},
    span::{AttrValue, KeyValue},
    wal,
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        metricsgen::{
            contract::{SpanKind, SpanRecord, StatusCode},
            series::{Series, SeriesPayload, SeriesSample},
        },
        span::{AttrValue, KeyValue, Span},
        wal,
    };

    fn payload() -> SeriesPayload {
        SeriesPayload {
            tenant: "t".into(),
            series: vec![Series {
                name: "traces_spanmetrics_calls_total".into(),
                labels: vec![("service".into(), "api".into())],
                sample: SeriesSample::Counter(1.0),
                exemplars: vec![],
                timestamp_ms: 1_000,
            }],
        }
    }

    fn span() -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: [0; 8],
            name: "op".into(),
            kind: SpanKind::Server,
            start_ns: 0,
            duration_ns: 1,
            status: StatusCode::Ok,
            status_message: String::new(),
            service_name: "api".into(),
            attributes: vec![],
            size: ByteSize::from_bytes(0),
        }
    }

    fn wal_span() -> Span {
        Span {
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: Some([0xEF; 8]),
            name: "GET /checkout".into(),
            kind: SpanKind::Server,
            start_ns: 10,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("checkout".into()),
            }],
            span_attrs: vec![
                KeyValue {
                    key: "db.system".into(),
                    value: AttrValue::Str("postgresql".into()),
                },
                KeyValue {
                    key: "http.status_code".into(),
                    value: AttrValue::Int(200),
                },
            ],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[tokio::test]
    async fn mock_sink_records_writes_and_can_fail_once() {
        let sink = MockRemoteWriteSink::default();
        sink.fail_next();
        check!(sink.write(&payload()).await.is_err());
        check!(sink.write(&payload()).await.is_ok());
        check!(sink.writes().len() == 1);
    }

    #[tokio::test]
    async fn mock_source_returns_scripted_batches_and_tracks_commits() {
        let src = MockSpanSource::default();
        src.push_batch(vec![span(), span()]);
        let batch = src.poll(10).await.unwrap();
        assert2::assert!(batch.len() == 2);
        assert2::assert!(src.poll(10).await.unwrap().is_empty());
        src.commit().await.unwrap();
        assert2::assert!(src.commits() == 1);
    }

    #[test]
    fn wal_record_projects_to_metricsgen_contract() {
        let record = wal::SpanRecord {
            tenant: "tenant-a".into(),
            span: wal_span(),
        };

        let projected = project_wal_record(record, ByteSize::from_bytes(123));

        assert2::assert!(
            projected
                == SpanRecord {
                    tenant: "tenant-a".into(),
                    trace_id: [0xAB; 16],
                    span_id: [0xCD; 8],
                    parent_span_id: [0xEF; 8],
                    name: "GET /checkout".into(),
                    kind: SpanKind::Server,
                    start_ns: 10,
                    duration_ns: 5_000_000,
                    status: StatusCode::Ok,
                    status_message: String::new(),
                    service_name: "checkout".into(),
                    attributes: vec![
                        ("db.system".into(), "postgresql".into()),
                        ("http.status_code".into(), "200".into()),
                    ],
                    size: ByteSize::from_bytes(123),
                }
        );
    }

    #[test]
    fn consumer_records_decode_wal_values_and_skip_tombstones() {
        let record = wal::SpanRecord {
            tenant: "tenant-a".into(),
            span: wal_span(),
        };
        let encoded = record.encode().unwrap();
        let records = vec![
            krabka_client_consumer::ConsumerRecord {
                topic: crate::TRACES_WAL_TOPIC.into(),
                partition: 0,
                offset: 1,
                leader_epoch: -1,
                timestamp: 0,
                key: None,
                value: Some(bytes::Bytes::from(encoded.clone())),
                headers: Vec::new(),
            },
            krabka_client_consumer::ConsumerRecord {
                topic: crate::TRACES_WAL_TOPIC.into(),
                partition: 0,
                offset: 2,
                leader_epoch: -1,
                timestamp: 0,
                key: None,
                value: None,
                headers: Vec::new(),
            },
        ];

        let projected = decode_consumer_records(records).unwrap();

        assert2::assert!(projected.len() == 1);
        check!(projected[0].tenant == "tenant-a");
        check!(projected[0].size.bytes_usize() == encoded.len());
    }
}

// === split-modules: generated submodules ===
mod attr_value_to_string;
mod decode_consumer_records;
mod kafka_span_source;
mod mock_remote_write_sink;
mod mock_span_source;
mod project_wal_record;
mod remote_write_sink;
mod service_name;
mod sink_error;
mod span_source;

use attr_value_to_string::attr_value_to_string;
pub use decode_consumer_records::decode_consumer_records;
pub use kafka_span_source::KafkaSpanSource;
pub use mock_remote_write_sink::MockRemoteWriteSink;
pub use mock_span_source::MockSpanSource;
pub use project_wal_record::project_wal_record;
pub use remote_write_sink::RemoteWriteSink;
use service_name::service_name;
pub use sink_error::SinkError;
pub use span_source::SpanSource;
