//! `remote_write` v1 (`prometheus.WriteRequest`) request decoder.

use std::collections::HashSet;

use krabka_blockstore::{LabelPair, Labels};
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
    use crate::wire::test_support::{
        assert_duplicate_job_label, check_up_sample_with_trace_exemplar, snappy, up_v1_series,
    };

    #[test]
    fn decodes_v1_samples_and_exemplars() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                exemplars: vec![pb::v1::Exemplar {
                    labels: vec![pb::v1::Label {
                        name: "trace_id".into(),
                        value: "abc".into(),
                    }],
                    value: 2.0,
                    timestamp: 1100,
                }],
                ..up_v1_series(pb::v1::Sample {
                    value: 1.0,
                    timestamp: 1000,
                })
            }],
            metadata: Vec::new(),
        };

        let decoded = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        check_up_sample_with_trace_exemplar(&decoded);
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
        check!(decoded[0].histograms[0].timestamp_ms == 10);
        check!(decoded[0].histograms[0].histogram.positive_counts == vec![1.0, 3.0]);
    }

    #[test]
    fn decode_v1_rejects_duplicate_label_names() {
        let mut series = up_v1_series(pb::v1::Sample {
            value: 1.0,
            timestamp: 1000,
        });
        series.labels.extend([
            pb::v1::Label {
                name: "job".into(),
                value: "api".into(),
            },
            pb::v1::Label {
                name: "job".into(),
                value: "worker".into(),
            },
        ]);
        let req = pb::v1::WriteRequest {
            timeseries: vec![series],
            ..Default::default()
        };

        let err = decode_v1(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap_err();

        assert_duplicate_job_label(&err);
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
