//! `remote_write` v2 (`io.prometheus.write.v2.Request`) request decoder.

use krabka_blockstore::Labels;
use krabka_units::prelude::*;
use prost::Message;

use super::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, WireError,
    histogram::v2_histogram_to_native, pb, snappy_block_decode,
};
use crate::SymbolTable;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use prost::Message;

    use super::*;

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decodes_v2_symbols_samples_exemplars_and_counts() {
        let req = pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "trace_id".into(),
                "abc".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                exemplars: vec![pb::v2::Exemplar {
                    labels_refs: vec![3, 4],
                    value: 2.0,
                    timestamp: 1100,
                }],
                ..Default::default()
            }],
        };

        let (decoded, counts) = decode_v2(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        assert!(decoded.len() == 1);
        check!(decoded[0].labels.get("__name__") == Some("up"));
        check!(decoded[0].samples == vec![DecodedSample::new(1000, 1.0)]);
        check!(decoded[0].exemplars[0].labels.get("trace_id") == Some("abc"));
        check!(
            counts
                == WrittenCounts {
                    samples: 1,
                    histograms: 0,
                    exemplars: 1,
                }
        );
    }

    #[test]
    fn decodes_v2_histograms_and_counts_them() {
        let req = pb::v2::Request {
            symbols: vec![String::new()],
            timeseries: vec![pb::v2::TimeSeries {
                histograms: vec![pb::v2::Histogram {
                    timestamp: 10,
                    positive_spans: vec![pb::v2::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![1, 2],
                    count: Some(pb::v2::histogram::Count::CountInt(3)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let (decoded, counts) = decode_v2(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        assert!(counts.histograms == 1);
        assert!(decoded[0].histograms[0].1.positive_counts == vec![1.0, 3.0]);
    }

    #[test]
    fn decode_v2_rejects_non_empty_first_symbol() {
        let req = pb::v2::Request {
            symbols: vec!["not-empty".into()],
            timeseries: Vec::new(),
        };

        let err = decode_v2(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
    }

    #[test]
    fn decode_v2_rejects_duplicate_label_names() {
        let req = pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "job".into(),
                "api".into(),
                "worker".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 3, 5],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };

        let err = decode_v2(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("duplicate label `job`"));
    }

    #[test]
    fn decodes_v2_sample_start_timestamp() {
        let req = pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 500,
                }],
                ..Default::default()
            }],
        };

        let (decoded, _) = decode_v2(&snappy(&req.encode_to_vec()), mebibytes(1)).unwrap();

        assert!(
            decoded[0].samples == vec![DecodedSample::with_start_timestamp(1000, 1.0, Some(500))]
        );
    }
}

// === split-modules: generated submodules ===
mod decode_v2;
mod labels_from_refs;
mod metadata_from_v2;
mod metadata_type;
mod symbol_ref;
mod written_counts;

pub use decode_v2::decode_v2;
use labels_from_refs::labels_from_refs;
use metadata_from_v2::metadata_from_v2;
use metadata_type::metadata_type;
use symbol_ref::symbol_ref;
pub use written_counts::WrittenCounts;
