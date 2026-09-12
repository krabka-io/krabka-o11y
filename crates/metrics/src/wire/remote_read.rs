//! Prometheus `remote_read` protobuf helpers.
//!
//! This module implements both `SAMPLES` and `STREAMED_XOR_CHUNKS` responses
//! for the v1 read format.

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

    #[test]
    fn streamed_response_negotiation_uses_fifo_order_and_samples_default() {
        check!(negotiate_read_response_type(&[]).unwrap() == v1::ResponseType::Samples);
        check!(
            negotiate_read_response_type(&[
                99,
                v1::ResponseType::StreamedXorChunks as i32,
                v1::ResponseType::Samples as i32,
            ])
            .unwrap()
                == v1::ResponseType::StreamedXorChunks
        );
        check!(
            negotiate_read_response_type(&[
                v1::ResponseType::Samples as i32,
                v1::ResponseType::StreamedXorChunks as i32,
            ])
            .unwrap()
                == v1::ResponseType::Samples
        );
        assert!(matches!(
            negotiate_read_response_type(&[99]),
            Err(RemoteReadError::UnsupportedResponseTypes(types)) if types == vec![99]
        ));
    }

    #[test]
    fn xor_chunk_matches_prometheus_single_sample_fixture() {
        let chunks = encode_xor_chunks(&[v1::Sample {
            timestamp: 7_200_000,
            value: 12_000.0,
        }])
        .unwrap();

        assert!(chunks.len() == 1);
        check!(chunks[0].min_time_ms == 7_200_000);
        check!(chunks[0].max_time_ms == 7_200_000);
        check!(chunks[0].r#type == v1::chunk::Encoding::Xor as i32);
        assert!(
            chunks[0].data
                == vec![
                    0x00, 0x01, 0x80, 0xf4, 0xee, 0x06, 0x40, 0xc7, 0x70, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ]
        );
    }

    #[test]
    fn streamed_frames_have_uvarint_length_big_endian_crc32c_and_query_index() {
        let response = v1::ReadResponse {
            results: vec![v1::QueryResult {
                timeseries: vec![v1::TimeSeries {
                    labels: vec![v1::Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    }],
                    samples: vec![v1::Sample {
                        timestamp: 42,
                        value: 7.0,
                    }],
                    ..Default::default()
                }],
            }],
        };

        let frames = encode_chunked_read_frames(response)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(frames.len() == 1);
        let (length, delimiter_len) = read_uvarint(&frames[0]);
        let checksum = u32::from_be_bytes(
            frames[0][delimiter_len..delimiter_len + 4]
                .try_into()
                .unwrap(),
        );
        let payload = &frames[0][delimiter_len + 4..];
        check!(length == payload.len() as u64);
        check!(checksum == crc32c::crc32c(payload));

        let decoded = v1::ChunkedReadResponse::decode(payload).unwrap();
        check!(decoded.query_index == 0);
        assert!(decoded.chunked_series.len() == 1);
        assert!(decoded.chunked_series[0].chunks.len() == 1);
    }

    #[test]
    fn xor_series_split_at_prometheus_chunk_sample_limit() {
        let samples = (0..121)
            .map(|timestamp| v1::Sample {
                timestamp: i64::from(timestamp),
                value: f64::from(timestamp),
            })
            .collect::<Vec<_>>();

        let chunks = encode_xor_chunks(&samples).unwrap();

        assert!(chunks.len() == 2);
        check!(chunks[0].min_time_ms == 0);
        check!(chunks[0].max_time_ms == 119);
        check!(chunks[1].min_time_ms == 120);
        check!(chunks[1].max_time_ms == 120);
    }

    fn read_uvarint(bytes: &[u8]) -> (u64, usize) {
        let mut value = 0_u64;
        for (index, byte) in bytes.iter().copied().enumerate() {
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return (value, index + 1);
            }
        }
        panic!("unterminated uvarint")
    }
}

mod decode_read_request;
mod default_max_read_decompressed;
mod encode_chunked_read_frames;
mod encode_read_response;
mod encode_xor_chunk;
mod matchers_to_selectors;
mod negotiate_read_response_type;
mod remote_read_error;
mod series_to_timeseries;

#[cfg_attr(test, mutants::skip)]
pub use decode_read_request::decode_read_request;
pub use default_max_read_decompressed::DEFAULT_MAX_READ_DECOMPRESSED;
pub use encode_chunked_read_frames::encode_chunked_read_frames;
pub use encode_read_response::encode_read_response;
use encode_xor_chunk::encode_xor_chunks;
pub use matchers_to_selectors::matchers_to_selectors;
pub use negotiate_read_response_type::negotiate_read_response_type;
pub use remote_read_error::RemoteReadError;
pub use series_to_timeseries::series_to_timeseries;
