use assert2::assert;
use krabka_metrics::{
    NativeHistogram, ResetHint,
    distributor::wal_records_from_series,
    wal::{SamplePayload, WalExemplar, WalRecord},
    wire::{DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries},
};

#[test]
fn fanout_keeps_the_complete_record_ledger() {
    let labels = vec![
        ("__name__".to_string(), "rpc_seconds_sum".into()),
        ("job".to_string(), "api".into()),
    ];
    let histogram = native_histogram();
    let record = |payload| WalRecord {
        tenant: "tenant-a".to_string(),
        labels: labels.clone(),
        payload,
        exemplars: Vec::new(),
    };
    let floats = [
        record(SamplePayload::Float {
            timestamp_ms: 10,
            value: 1.0,
            start_timestamp_ms: Some(3),
        }),
        record(SamplePayload::Float {
            timestamp_ms: 20,
            value: 2.0,
            start_timestamp_ms: Some(5),
        }),
    ];
    let histograms = [
        record(SamplePayload::Hist {
            timestamp_ms: 30,
            hist: histogram.clone(),
        }),
        record(SamplePayload::Hist {
            timestamp_ms: 40,
            hist: histogram.clone(),
        }),
    ];
    let metadata = WalRecord {
        labels: vec![
            ("__name__".to_string(), "rpc_seconds".into()),
            ("job".to_string(), "api".into()),
        ],
        ..record(SamplePayload::Metadata {
            metric_family_name: "rpc_seconds".to_string(),
            metric_type: "summary".to_string(),
            help: "Request duration".to_string(),
            unit: "seconds".to_string(),
        })
    };
    let exemplar = WalRecord {
        exemplars: vec![WalExemplar {
            labels: vec![("trace_id".to_string(), "abc".to_string())],
            value: 0.25,
            timestamp_ms: 5,
        }],
        ..record(SamplePayload::Exemplars)
    };

    for (float_count, histogram_count, has_metadata, has_exemplar) in (0..=2).flat_map(|f| {
        (0..=2).flat_map(move |h| {
            [false, true]
                .into_iter()
                .flat_map(move |m| [false, true].into_iter().map(move |e| (f, h, m, e)))
        })
    }) {
        let series = DecodedSeries {
            labels: [("__name__", "rpc_seconds_sum"), ("job", "api")]
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            samples: [
                DecodedSample::with_start_timestamp(10, 1.0, Some(3)),
                DecodedSample::with_start_timestamp(20, 2.0, Some(5)),
            ][..float_count]
                .to_vec(),
            histograms: [(30, histogram.clone()), (40, histogram.clone())][..histogram_count]
                .to_vec(),
            metadata: has_metadata.then(|| DecodedMetadata {
                metric_family_name: "rpc_seconds".to_string(),
                metric_type: "summary".to_string(),
                help: "Request duration".to_string(),
                unit: "seconds".to_string(),
            }),
            exemplars: if has_exemplar {
                vec![DecodedExemplar {
                    labels: [("trace_id", "abc")]
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect(),
                    timestamp_ms: 5,
                    value: 0.25,
                }]
            } else {
                Vec::new()
            },
        };
        let mut expected = floats[..float_count].to_vec();
        expected.extend_from_slice(&histograms[..histogram_count]);
        if has_metadata {
            expected.push(metadata.clone());
        }
        if has_exemplar {
            expected.push(exemplar.clone());
        }
        assert!(wal_records_from_series("tenant-a", &[series]) == expected);
    }
}

fn native_histogram() -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: false,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 3.0,
        count: 3.0,
        sum: 7.0,
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: Some(4),
    }
}
