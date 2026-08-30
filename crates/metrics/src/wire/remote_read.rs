//! Prometheus `remote_read` protobuf helpers.
//!
//! This module implements the SAMPLES response path for the v1 read format.
//! It deliberately does not advertise or encode `STREAMED_XOR_CHUNKS`.

use krabka_blockstore::{LabelMatcher, Labels, MatchOp};
use krabka_units::prelude::*;
use prost::Message;
use thiserror::Error;

use crate::wire::{decoded::snappy_block_decode_raw, pb::v1};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::{Labels, MatchOp};
    use prost::Message;

    use super::*;
    use crate::wire::pb::v1::{self, label_matcher};

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn read_request_snappy_round_trips() {
        let req = v1::ReadRequest {
            queries: vec![v1::Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: vec![v1::LabelMatcher {
                    r#type: label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "http_requests_total".into(),
                }],
                hints: None,
            }],
            accepted_response_types: Vec::new(),
        };

        let back =
            decode_read_request(&snappy(&req.encode_to_vec()), DEFAULT_MAX_READ_DECOMPRESSED)
                .unwrap();

        assert!(back.queries.len() == 1);
        let (selectors, start, end) = matchers_to_selectors(&back.queries[0]).unwrap();
        check!(start == 1000);
        check!(end == 2000);
        check!(selectors[0].name == "__name__");
        check!(selectors[0].op == MatchOp::Eq);
        check!(selectors[0].value == "http_requests_total");
    }

    /// A `remote_read` snappy block that declares a huge uncompressed length
    /// but carries a tiny payload must fail the declared-length pre-check,
    /// before `snap` allocates the declared buffer.
    #[test]
    fn read_request_rejects_declared_length_bomb() {
        // Hand-roll a raw snappy block: a varint preamble declaring ~1 GiB of
        // output followed by a one-byte literal.
        let huge: u64 = 1 << 30;
        let mut frame = Vec::new();
        let mut value = huge;
        while value >= 0x80 {
            frame.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        frame.push(u8::try_from(value).unwrap());
        frame.push(0x00);
        frame.push(0x42);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = decode_read_request(&frame, mebibytes(1)).unwrap_err();

        assert!(matches!(err, RemoteReadError::SnappyOutputTooLarge(_)));
    }

    #[test]
    fn samples_response_is_sorted() {
        let mut labels = Labels::new();
        labels.insert("job", "api");
        labels.insert("__name__", "x");
        let result = series_to_timeseries(vec![(labels, vec![(2_i64, 2.0_f64), (1, 1.0)])]);

        let ts = &result.timeseries[0];
        check!(ts.labels[0].name == "__name__");
        check!(ts.labels[1].name == "job");
        check!(ts.samples[0].timestamp == 1);
        check!(ts.samples[1].timestamp == 2);
    }

    #[test]
    fn response_encodes_as_snappy_protobuf() {
        let response = v1::ReadResponse {
            results: vec![v1::QueryResult {
                timeseries: vec![v1::TimeSeries {
                    samples: vec![v1::Sample {
                        timestamp: 42,
                        value: 7.0,
                    }],
                    ..Default::default()
                }],
            }],
        };

        let encoded = encode_read_response(&response).unwrap();
        let raw = snap::raw::Decoder::new().decompress_vec(&encoded).unwrap();
        let decoded = v1::ReadResponse::decode(raw.as_slice()).unwrap();

        assert!(decoded.results[0].timeseries[0].samples[0].timestamp == 42);
    }
}

// === split-modules: generated submodules ===
mod decode_read_request;
mod default_max_read_decompressed;
mod encode_read_response;
mod matchers_to_selectors;
mod remote_read_error;
mod series_to_timeseries;

# [cfg_attr (test , mutants :: skip)] pub use decode_read_request::decode_read_request;
pub use default_max_read_decompressed::DEFAULT_MAX_READ_DECOMPRESSED;
pub use encode_read_response::encode_read_response;
pub use matchers_to_selectors::matchers_to_selectors;
pub use remote_read_error::RemoteReadError;
pub use series_to_timeseries::series_to_timeseries;
