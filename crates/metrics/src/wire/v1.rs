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
    fn decode_v1_rejects_duplicate_label_names() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![
                    pb::v1::Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    },
                    pb::v1::Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                    pb::v1::Label {
                        name: "job".into(),
                        value: "worker".into(),
                    },
                ],
                samples: vec![pb::v1::Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("duplicate label `job`"));
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
