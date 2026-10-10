//! Traces WAL topic record shared by distributor, block-builder, and live-store.

use bytes::Bytes;
use krabka_blockstore::fnv1_32;
use serde::{Deserialize, Serialize};

use crate::{error::TracesError, span::Span};

#[cfg(test)]
mod tests {

    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value::Value};
    use prost::Message as _;

    use super::*;
    use crate::span::{AttrValue, KeyValue, test_span::api_server_span};

    fn span(trace_id: [u8; 16]) -> Span {
        Span {
            trace_id,
            duration_ns: 500,
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
            ..api_server_span()
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
    fn wal_preserves_nested_array_shape_and_double_bits() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let mut native = span([7; 16]);
        native.span_attrs.push(KeyValue {
            key: "nested".into(),
            value: AttrValue::Array(vec![
                AttrValue::Unsupported("{}".into()),
                AttrValue::Array(vec![
                    AttrValue::Double(nan),
                    AttrValue::Double(-0.0),
                    AttrValue::Int(i64::MAX),
                ]),
                AttrValue::Bytes(vec![0xff, 0]),
            ]),
        });
        let record = SpanRecord {
            tenant: "t1".into(),
            span: native,
        };
        let restored = SpanRecord::decode(&record.encode().unwrap()).unwrap();
        let expected = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue { value: None },
                    AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::DoubleValue(nan)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(-0.0)),
                                },
                                AnyValue {
                                    value: Some(Value::IntValue(i64::MAX)),
                                },
                            ],
                        })),
                    },
                    AnyValue {
                        value: Some(Value::BytesValue(vec![0xff, 0])),
                    },
                ],
            })),
        };
        assert2::check!(
            restored.span.span_attrs[0]
                .value
                .otlp_value()
                .encode_to_vec()
                == expected.encode_to_vec()
        );
        assert2::check!(restored.tenant == "t1");
        assert2::check!(restored.span.trace_id == [7; 16]);
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
