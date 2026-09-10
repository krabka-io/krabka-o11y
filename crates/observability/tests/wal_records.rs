//! Kafka WAL records, and the headers a log row encodes into.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_blockstore::{labels, series_fingerprint};
use krabka_observability::{
    KafkaWalHeader, KafkaWalRecord, Offset, PartitionIndex, WalLogRecord, WalPosition,
    build_kafka_wal_record, decode_kafka_wal_record, decode_kafka_wal_record_envelope,
};
use serde_json::json;

#[test]
fn kafka_wal_record_encodes_tenant_series_key_headers_and_json_payload() {
    let labels = labels([("app", "api"), ("env", "prod")]);
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels.clone(),
        timestamp_ns: 1_900_000,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: Some(WalPosition {
            partition: PartitionIndex(3),
            offset: Offset(42),
        }),
    };

    let producer_record = build_kafka_wal_record("__krabka_observability_logs_wal", &record)
        .expect("producer record");

    check!(producer_record.topic == "__krabka_observability_logs_wal");
    check!(
        producer_record.key.as_deref()
            == Some(format!("tenant-a:{}", series_fingerprint(&labels)).as_bytes())
    );
    check!(producer_record.timestamp_ms == Some(1));
    check!(
        producer_record
            .headers
            .iter()
            .any(|header| header.key == "krabka-wal-record-type"
                && header.value.as_deref() == Some(b"log".as_slice()))
    );
    check!(
        producer_record
            .headers
            .iter()
            .any(|header| header.key == "krabka-tenant"
                && header.value.as_deref() == Some(b"tenant-a".as_slice()))
    );

    let payload: serde_json::Value =
        serde_json::from_slice(producer_record.value.as_deref().unwrap()).unwrap();
    assert!(
        payload
            == json!({
                "tenant": "tenant-a",
                "labels": {"app": "api", "env": "prod"},
                "timestamp_ns": 1_900_000,
                "line": "api error",
                "structured_metadata": {"trace_id": "abc"},
                "position": {"partition": 3, "offset": 42},
            })
    );
}

#[test]
fn kafka_wal_record_decodes_payload_with_consumed_position() {
    let labels = labels([("app", "api"), ("env", "prod")]);
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels.clone(),
        timestamp_ns: 1_900_000,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: None,
    };

    let producer_record = build_kafka_wal_record("__krabka_observability_logs_wal", &record)
        .expect("producer record");
    let decoded = decode_kafka_wal_record(
        producer_record.value.as_deref().unwrap(),
        PartitionIndex(7),
        Offset(99),
    )
    .expect("decoded WAL record");

    assert!(
        decoded
            == WalLogRecord {
                position: Some(WalPosition {
                    partition: PartitionIndex(7),
                    offset: Offset(99),
                }),
                ..record
            }
    );
}

#[test]
fn kafka_wal_record_decode_rejects_invalid_payload() {
    let error = decode_kafka_wal_record(b"not json", PartitionIndex(7), Offset(99)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
}

#[test]
fn native_kafka_log_record_rejects_invalid_label_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-9bad".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka label"));
}

#[test]
fn native_kafka_log_record_rejects_invalid_metadata_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-metadata-9bad".to_string(),
                value: Some(b"metadata".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka metadata"));
}

#[test]
fn native_kafka_log_record_rejects_duplicate_label_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"worker".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("duplicate native Kafka label"));
}

#[test]
fn native_kafka_log_record_rejects_duplicate_metadata_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-metadata-trace_id".to_string(),
                value: Some(b"abc".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-metadata-trace_id".to_string(),
                value: Some(b"def".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("duplicate native Kafka metadata")
    );
}

#[test]
fn native_kafka_log_record_rejects_negative_timestamp_header() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-timestamp-ns".to_string(),
                value: Some(b"-1".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}

#[test]
fn native_kafka_log_record_rejects_negative_broker_timestamp() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(-1),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}

#[test]
fn native_kafka_log_record_rejects_broker_timestamp_overflow() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: Some(i64::MAX),
        headers: vec![
            KafkaWalHeader {
                key: "krabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "krabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}
