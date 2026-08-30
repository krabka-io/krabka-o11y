//! Traces WAL topic record shared by distributor, block-builder, and live-store.

use bytes::Bytes;
use krabka_blockstore::fnv1_32;
use serde::{Deserialize, Serialize};

use crate::{error::TracesError, span::Span};

#[cfg(test)]
mod tests {

    use super::*;
    use crate::span::{AttrValue, KeyValue, SpanKind, StatusCode};

    fn span(trace_id: [u8; 16]) -> Span {
        Span {
            trace_id,
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[test]
    fn record_round_trips() {
        let rec = SpanRecord {
            tenant: "t1".into(),
            span: span([7; 16]),
        };
        let bytes = rec.encode().unwrap();
        let back = SpanRecord::decode(&bytes).unwrap();
        assert2::assert!(back == rec);
    }

    #[test]
    fn same_trace_id_same_partition_key() {
        let trace_id = [9; 16];
        let k1 = partition_key(&trace_id);
        let k2 = partition_key(&trace_id);
        let k3 = partition_key(&[10; 16]);
        assert2::assert!(k1 == k2);
        assert2::assert!(k1 != k3);
    }

    #[test]
    fn partition_key_is_trace_id_hash() {
        let trace_id = [9; 16];
        let key = partition_key(&trace_id);
        let expected = krabka_blockstore::fnv1_32(&trace_id).to_be_bytes();

        assert2::assert!(key.as_ref() == expected);
    }

    #[test]
    fn wal_topic_matches_spec() {
        assert2::assert!(TRACES_WAL_TOPIC == "__krabka_traces_wal");
    }
}

mod partition_key;
mod span_record;
mod traces_wal_topic;

pub use partition_key::partition_key;
pub use span_record::SpanRecord;
pub use traces_wal_topic::TRACES_WAL_TOPIC;
