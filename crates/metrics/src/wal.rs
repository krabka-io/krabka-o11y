//! Metrics WAL topic record shared by ingest, compaction, and query.

use bytes::Bytes;
use krabka_blockstore::Labels;
use krabka_units::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    NativeHistogram,
    wire::{DecodedClockReading, UnixNanos},
};

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{BucketSpan, ResetHint};

    /// `timestamp_ms` answers for the two payloads that carry a timestamp and
    /// declines for the two that do not. The two timestamps differ, so a
    /// variant reading the other's field is visible rather than merely
    /// returning a plausible number.
    #[test]
    fn only_the_sample_payloads_that_carry_a_timestamp_report_one() {
        use super::SamplePayload;

        let float = SamplePayload::Float {
            timestamp_ms: 11,
            value: 1.5,
            start_timestamp_ms: Some(99),
        };
        let histogram = SamplePayload::Hist {
            timestamp_ms: 22,
            hist: hist(),
        };
        let metadata = SamplePayload::Metadata {
            metric_family_name: "m".into(),
            metric_type: "counter".into(),
            help: String::new(),
            unit: String::new(),
        };

        assert!(float.timestamp_ms() == Some(11));
        assert!(histogram.timestamp_ms() == Some(22));
        assert!(metadata.timestamp_ms() == None);
        assert!(SamplePayload::Exemplars.timestamp_ms() == None);

        // A float's start timestamp is a different field and must not be
        // returned in place of its timestamp.
        assert!(float.timestamp_ms() != Some(99));

        // Zero and negative are values, not absences.
        let epoch = SamplePayload::Float {
            timestamp_ms: 0,
            value: 0.0,
            start_timestamp_ms: None,
        };
        assert!(epoch.timestamp_ms() == Some(0), "the epoch is a timestamp");
        let before = SamplePayload::Float {
            timestamp_ms: -1,
            value: 0.0,
            start_timestamp_ms: None,
        };
        assert!(before.timestamp_ms() == Some(-1));
    }

    fn hist() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 7.0,
            sum: 3.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn float_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![
                ("__name__".into(), "up".into()),
                ("job".into(), "api".into()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms: 100,
                value: 1.5,
                start_timestamp_ms: Some(50),
            },
            exemplars: Vec::new(),
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn hist_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "latency".into())],
            payload: SamplePayload::Hist {
                timestamp_ms: 200,
                hist: hist(),
            },
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".into(), "abc".into())],
                value: 0.9,
                timestamp_ms: 200,
            }],
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn exemplar_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "requests_total".into())],
            payload: SamplePayload::Exemplars,
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".into(), "abc".into())],
                value: 0.9,
                timestamp_ms: 200,
            }],
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = WalRecord {
            tenant: "t".into(),
            labels: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            payload: SamplePayload::Float {
                timestamp_ms: 0,
                value: 0.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let mut b = a.clone();
        b.labels = vec![("b".into(), "2".into()), ("a".into(), "1".into())];

        assert!(a.series_fingerprint() == b.series_fingerprint());
    }

    #[test]
    fn partition_key_is_stable() {
        let k1 = partition_key("t", 42);
        let k2 = partition_key("t", 42);
        let k3 = partition_key("t", 43);

        assert!(k1 == k2);
        assert!(k1 != k3);
    }
}

// === split-modules: generated submodules ===
mod clock_reading_payload;
mod partition_key;
mod sample_payload;
mod wal_error;
mod wal_exemplar;
mod wal_record;
mod wal_topic;

pub use clock_reading_payload::ClockReadingPayload;
pub use partition_key::partition_key;
pub use sample_payload::SamplePayload;
pub use wal_error::WalError;
pub use wal_exemplar::WalExemplar;
pub use wal_record::WalRecord;
pub use wal_topic::WAL_TOPIC;
