use assert2::assert;
use krabka_metrics::{
    MetricLabelPair, NativeHistogram, ResetHint,
    distributor::wal_records_from_series,
    wal::{SamplePayload, WalExemplar, WalRecord},
    wire::{
        DecodedExemplar, DecodedHistogramSample, DecodedMetadata, DecodedSample, DecodedSeries,
    },
};

struct FanoutCase {
    float_count: usize,
    histogram_count: usize,
    has_metadata: bool,
    has_exemplar: bool,
}

#[test]
fn fanout_keeps_the_complete_record_ledger() {
    let labels = vec![
        MetricLabelPair::new("__name__", "rpc_seconds_sum".into()),
        MetricLabelPair::new("job", "api".into()),
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
            MetricLabelPair::new("__name__", "rpc_seconds".into()),
            MetricLabelPair::new("job", "api".into()),
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
            labels: vec![MetricLabelPair::new("trace_id", "abc".to_string())],
            value: 0.25,
            timestamp_ms: 5,
        }],
        ..record(SamplePayload::Exemplars)
    };

    for case in (0..=2).flat_map(|float_count| {
        (0..=2).flat_map(move |h| {
            [false, true].into_iter().flat_map(move |has_metadata| {
                [false, true]
                    .into_iter()
                    .map(move |has_exemplar| FanoutCase {
                        float_count,
                        histogram_count: h,
                        has_metadata,
                        has_exemplar,
                    })
            })
        })
    }) {
        let series = DecodedSeries {
            labels: krabka_blockstore::Labels::from_pairs([
                krabka_blockstore::LabelPair::new("__name__", "rpc_seconds_sum"),
                krabka_blockstore::LabelPair::new("job", "api"),
            ]),
            samples: [
                DecodedSample::with_start_timestamp(10, 1.0, Some(3)),
                DecodedSample::with_start_timestamp(20, 2.0, Some(5)),
            ][..case.float_count]
                .to_vec(),
            histograms: [
                DecodedHistogramSample {
                    timestamp_ms: 30,
                    histogram: histogram.clone(),
                },
                DecodedHistogramSample {
                    timestamp_ms: 40,
                    histogram: histogram.clone(),
                },
            ][..case.histogram_count]
                .to_vec(),
            metadata: case.has_metadata.then(|| DecodedMetadata {
                metric_family_name: "rpc_seconds".to_string(),
                metric_type: "summary".to_string(),
                help: "Request duration".to_string(),
                unit: "seconds".to_string(),
            }),
            exemplars: if case.has_exemplar {
                vec![DecodedExemplar {
                    labels: krabka_blockstore::Labels::from_pairs([
                        krabka_blockstore::LabelPair::new("trace_id", "abc"),
                    ]),
                    timestamp_ms: 5,
                    value: 0.25,
                }]
            } else {
                Vec::new()
            },
        };
        let mut expected = floats[..case.float_count].to_vec();
        expected.extend_from_slice(&histograms[..case.histogram_count]);
        if case.has_metadata {
            expected.push(metadata.clone());
        }
        if case.has_exemplar {
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
