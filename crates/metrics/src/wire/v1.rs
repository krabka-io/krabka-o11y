//! `remote_write` v1 (`prometheus.WriteRequest`) request decoder.

use std::collections::HashSet;

use krabka_blockstore::Labels;
use krabka_units::prelude::*;
use prost::Message;

use super::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, WireError,
    histogram::v1_histogram_to_native, pb, snappy_block_decode,
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use prost::Message;

    use super::*;
    use crate::{SamplePayload, WalExemplar, WalRecord, distributor::wal_records_from_series};

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decodes_v1_samples_and_exemplars() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![pb::v1::Label {
                    name: "__name__".into(),
                    value: "up".into(),
                }],
                samples: vec![pb::v1::Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                exemplars: vec![pb::v1::Exemplar {
                    labels: vec![pb::v1::Label {
                        name: "trace_id".into(),
                        value: "abc".into(),
                    }],
                    value: 2.0,
                    timestamp: 1100,
                }],
                histograms: Vec::new(),
            }],
            metadata: Vec::new(),
        };

        let decoded = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        assert!(decoded.len() == 1);
        check!(decoded[0].labels.get("__name__") == Some("up"));
        check!(decoded[0].samples == vec![DecodedSample::new(1000, 1.0)]);
        check!(decoded[0].exemplars[0].labels.get("trace_id") == Some("abc"));
    }

    #[test]
    fn decodes_v1_histograms() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                histograms: vec![pb::v1::Histogram {
                    timestamp: 10,
                    positive_spans: vec![pb::v1::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![1, 2],
                    count: Some(pb::v1::histogram::Count::CountInt(3)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let decoded = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        assert!(decoded[0].histograms.len() == 1);
        check!(decoded[0].histograms[0].0 == 10);
        check!(decoded[0].histograms[0].1.positive_counts == vec![1.0, 3.0]);
    }

    #[test]
    fn owned_v1_labels_preserve_complete_decode_and_wal_records() {
        for empty in [false, true] {
            for reverse in [false, true] {
                let mut labels = if empty {
                    Vec::new()
                } else {
                    vec![("__name__", "requests"), ("empty", ""), ("é", "☃")]
                };
                let mut exemplar_labels = if empty {
                    Vec::new()
                } else {
                    vec![("blank", ""), ("trace_id", "東京")]
                };
                if reverse {
                    labels.reverse();
                    exemplar_labels.reverse();
                }
                let req = pb::v1::WriteRequest {
                    timeseries: vec![pb::v1::TimeSeries {
                        labels: labels
                            .iter()
                            .map(|(name, value)| pb::v1::Label {
                                name: (*name).into(),
                                value: (*value).into(),
                            })
                            .collect(),
                        samples: vec![
                            pb::v1::Sample {
                                timestamp: 1000,
                                value: 1.25,
                            },
                            pb::v1::Sample {
                                timestamp: 1100,
                                value: 2.5,
                            },
                        ],
                        exemplars: vec![
                            pb::v1::Exemplar {
                                labels: exemplar_labels
                                    .iter()
                                    .map(|(name, value)| pb::v1::Label {
                                        name: (*name).into(),
                                        value: (*value).into(),
                                    })
                                    .collect(),
                                timestamp: 1150,
                                value: 3.0,
                            },
                            pb::v1::Exemplar {
                                labels: vec![pb::v1::Label {
                                    name: "span_id".into(),
                                    value: "second".into(),
                                }],
                                timestamp: 1160,
                                value: 4.0,
                            },
                        ],
                        histograms: Vec::new(),
                    }],
                    metadata: vec![pb::v1::MetricMetadata {
                        r#type: pb::v1::metric_metadata::MetricType::Gauge as i32,
                        metric_family_name: "requests".into(),
                        help: "処理数".into(),
                        unit: "s".into(),
                    }],
                };
                let expected_labels: Labels = if empty {
                    Labels::new()
                } else {
                    Labels::from_pairs([("__name__", "requests"), ("empty", ""), ("é", "☃")])
                };
                let expected_exemplar_labels: Labels = if empty {
                    Labels::new()
                } else {
                    Labels::from_pairs([("blank", ""), ("trace_id", "東京")])
                };
                let expected = vec![
                    DecodedSeries {
                        labels: expected_labels,
                        samples: vec![
                            DecodedSample::new(1000, 1.25),
                            DecodedSample::new(1100, 2.5),
                        ],
                        histograms: Vec::new(),
                        exemplars: vec![
                            DecodedExemplar {
                                labels: expected_exemplar_labels,
                                timestamp_ms: 1150,
                                value: 3.0,
                            },
                            DecodedExemplar {
                                labels: Labels::from_pairs([("span_id", "second")]),
                                timestamp_ms: 1160,
                                value: 4.0,
                            },
                        ],
                        metadata: None,
                    },
                    DecodedSeries {
                        labels: Labels::from_pairs([("__name__", "requests")]),
                        samples: Vec::new(),
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: Some(DecodedMetadata {
                            metric_family_name: "requests".into(),
                            metric_type: "gauge".into(),
                            help: "処理数".into(),
                            unit: "s".into(),
                        }),
                    },
                ];
                let decoded = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();
                assert!(decoded == expected);

                let wal_labels = if empty {
                    Vec::new()
                } else {
                    vec![
                        ("__name__".into(), "requests".into()),
                        ("empty".into(), "".into()),
                        ("é".into(), "☃".into()),
                    ]
                };
                let expected_wal = vec![
                    WalRecord {
                        tenant: "tenant".into(),
                        labels: wal_labels.clone(),
                        payload: SamplePayload::Float {
                            timestamp_ms: 1000,
                            value: 1.25,
                            start_timestamp_ms: None,
                        },
                        exemplars: Vec::new(),
                    },
                    WalRecord {
                        tenant: "tenant".into(),
                        labels: wal_labels.clone(),
                        payload: SamplePayload::Float {
                            timestamp_ms: 1100,
                            value: 2.5,
                            start_timestamp_ms: None,
                        },
                        exemplars: Vec::new(),
                    },
                    WalRecord {
                        tenant: "tenant".into(),
                        labels: wal_labels,
                        payload: SamplePayload::Exemplars,
                        exemplars: vec![
                            WalExemplar {
                                labels: if empty {
                                    Vec::new()
                                } else {
                                    vec![
                                        ("blank".into(), String::new()),
                                        ("trace_id".into(), "東京".into()),
                                    ]
                                },
                                timestamp_ms: 1150,
                                value: 3.0,
                            },
                            WalExemplar {
                                labels: vec![("span_id".into(), "second".into())],
                                timestamp_ms: 1160,
                                value: 4.0,
                            },
                        ],
                    },
                    WalRecord {
                        tenant: "tenant".into(),
                        labels: vec![("__name__".into(), "requests".into())],
                        payload: SamplePayload::Metadata {
                            metric_family_name: "requests".into(),
                            metric_type: "gauge".into(),
                            help: "処理数".into(),
                            unit: "s".into(),
                        },
                        exemplars: Vec::new(),
                    },
                ];
                let records = wal_records_from_series("tenant", &decoded);
                assert!(records == expected_wal);
                let replayed = records
                    .iter()
                    .map(|record| WalRecord::decode(&record.encode().unwrap()).unwrap())
                    .collect::<Vec<_>>();
                assert!(replayed == expected_wal);
            }
        }
    }

    #[test]
    fn decode_v1_rejects_duplicate_label_names_in_wire_order() {
        let duplicate_labels = || {
            [("job", "api"), ("a", "1"), ("job", "worker"), ("a", "2")]
                .into_iter()
                .map(|(name, value)| pb::v1::Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect()
        };
        let bad_histogram = || pb::v1::Histogram {
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 1,
            }],
            ..Default::default()
        };
        let duplicate_exemplar = || pb::v1::Exemplar {
            labels: duplicate_labels(),
            ..Default::default()
        };
        let cases = [
            (
                vec![pb::v1::TimeSeries {
                    labels: duplicate_labels(),
                    histograms: vec![bad_histogram()],
                    exemplars: vec![duplicate_exemplar()],
                    ..Default::default()
                }],
                "duplicate label `job`",
            ),
            (
                vec![pb::v1::TimeSeries {
                    exemplars: vec![duplicate_exemplar()],
                    ..Default::default()
                }],
                "duplicate label `job`",
            ),
            (
                vec![pb::v1::TimeSeries {
                    histograms: vec![bad_histogram()],
                    exemplars: vec![duplicate_exemplar()],
                    ..Default::default()
                }],
                "positive spans declare 1 buckets but 0 counts were decoded",
            ),
            (
                vec![
                    pb::v1::TimeSeries {
                        exemplars: vec![pb::v1::Exemplar {
                            labels: vec![
                                pb::v1::Label {
                                    name: "earlier".into(),
                                    value: "1".into(),
                                },
                                pb::v1::Label {
                                    name: "earlier".into(),
                                    value: "2".into(),
                                },
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    pb::v1::TimeSeries {
                        labels: duplicate_labels(),
                        ..Default::default()
                    },
                ],
                "duplicate label `earlier`",
            ),
        ];
        for (timeseries, expected) in cases {
            let req = pb::v1::WriteRequest {
                timeseries,
                ..Default::default()
            };
            let err = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap_err();
            assert!(matches!(err, WireError::Invalid(ref message) if message == expected));
        }
    }
}

mod decode_v1;
mod labels_from_v1;
mod metadata_series_from_v1;
mod metadata_type;

pub use decode_v1::decode_v1;
use labels_from_v1::labels_from_v1;
use metadata_series_from_v1::metadata_series_from_v1;
use metadata_type::metadata_type;
