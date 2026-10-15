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


    use super::*;
    use crate::wire::{
        pb::v1::{self, label_matcher},
        test_support::declared_length_bomb,
    };

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
        let selectors = matchers_to_selectors(&back.queries[0]).unwrap();
        check!(selectors.start_timestamp_ms == 1000);
        check!(selectors.end_timestamp_ms == 2000);
        check!(selectors.selectors[0].name == "__name__");
        check!(selectors.selectors[0].op == MatchOp::Eq);
        check!(selectors.selectors[0].value == "http_requests_total");
    }

    /// A `remote_read` snappy block that declares a huge uncompressed length
    /// but carries a tiny payload must fail the declared-length pre-check,
    /// before `snap` allocates the declared buffer.
    #[test]
    fn read_request_rejects_declared_length_bomb() {
        // ~1 GiB declared, one byte carried.
        let huge: u64 = 1 << 30;
        let frame = declared_length_bomb(huge);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = decode_read_request(&frame, mebibytes(1)).unwrap_err();

        assert!(matches!(err, RemoteReadError::SnappyOutputTooLarge(_)));
    }

    #[test]
    fn samples_response_is_sorted() {
        let mut labels = Labels::new();
        labels.insert("job", "api");
        labels.insert("__name__", "x");
        let result = series_to_timeseries(vec![RemoteReadSeries {
            labels,
            samples: vec![
                RemoteReadSample {
                    timestamp_ms: 2,
                    value: 2.0,
                },
                RemoteReadSample {
                    timestamp_ms: 1,
                    value: 1.0,
                },
            ],
        }]);

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
        let length = read_uvarint(&frames[0]);
        let checksum = u32::from_be_bytes(
            frames[0][length.encoded_len..length.encoded_len + 4]
                .try_into()
                .unwrap(),
        );
        let payload = &frames[0][length.encoded_len + 4..];
        check!(length.value == payload.len() as u64);
        check!(checksum == crc32c::crc32c(payload));

        let decoded = v1::ChunkedReadResponse::decode(payload).unwrap();
        check!(decoded.query_index == 0);
        assert!(decoded.chunked_series.len() == 1);
        assert!(decoded.chunked_series[0].chunks.len() == 1);
    }

    #[test]
    fn streamed_frames_encode_native_histogram_chunks() {
        let response = v1::ReadResponse {
            results: vec![v1::QueryResult {
                timeseries: vec![v1::TimeSeries {
                    histograms: vec![v1::Histogram {
                        count: Some(v1::histogram::Count::CountFloat(4.0)),
                        sum: 10.0,
                        schema: 0,
                        zero_threshold: 0.0,
                        zero_count: Some(v1::histogram::ZeroCount::ZeroCountFloat(0.0)),
                        reset_hint: v1::histogram::ResetHint::No as i32,
                        timestamp: 10_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }],
        };

        let frames = encode_chunked_read_frames(response)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let delimiter = read_uvarint(&frames[0]);
        let decoded =
            v1::ChunkedReadResponse::decode(&frames[0][delimiter.encoded_len + 4..]).unwrap();
        let chunk = &decoded.chunked_series[0].chunks[0];

        check!(chunk.r#type == v1::chunk::Encoding::FloatHistogram as i32);
        check!(chunk.min_time_ms == 10_000);
        check!(chunk.max_time_ms == 10_000);
        assert!(chunk.data.starts_with(&[0, 1, 0x40]));
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

    struct DecodedVarint {
        value: u64,
        encoded_len: usize,
    }

    fn read_uvarint(bytes: &[u8]) -> DecodedVarint {
        let mut value = 0_u64;
        for byte_entry in bytes.iter().copied().enumerate() {
            let index = byte_entry.0;
            let byte = byte_entry.1;
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return DecodedVarint {
                    value,
                    encoded_len: index + 1,
                };
            }
        }
        panic!("unterminated uvarint")
    }
}

#[cfg_attr(test, mutants::skip)]

// cargo-mutants: covered by remote_read decode round-trip and snappy limit tests.
#[cfg_attr(test, mutants::skip)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_read_request(
    snappy_body: &[u8],
    max_output: ByteSize,
) -> Result<v1::ReadRequest, RemoteReadError> {
    let raw = snappy_block_decode_raw(
        snappy_body,
        max_output.bytes_usize(),
        RemoteReadError::SnappyDecode,
        RemoteReadError::SnappyOutputTooLarge,
    )?;
    v1::ReadRequest::decode(raw.as_slice())
        .map_err(|error| RemoteReadError::Decode(error.to_string()))
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_read_response(response: &impl Message) -> Result<Vec<u8>, RemoteReadError> {
    let mut raw = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut raw)
        .map_err(|error| RemoteReadError::Encode(error.to_string()))?;
    snap::raw::Encoder::new()
        .compress_vec(&raw)
        .map_err(|error| RemoteReadError::SnappyEncode(error.to_string()))
}

/// Chooses the first response type in the client's FIFO preference list that
/// this server implements. An absent list means the legacy `SAMPLES` format.
///
/// # Errors
///
/// Returns [`RemoteReadError::UnsupportedResponseTypes`] when the list contains
/// no supported response type.
pub fn negotiate_read_response_type(accepted: &[i32]) -> Result<v1::ResponseType, RemoteReadError> {
    if accepted.is_empty() {
        return Ok(v1::ResponseType::Samples);
    }
    accepted
        .iter()
        .find_map(|response_type| match *response_type {
            value if value == v1::ResponseType::Samples as i32 => Some(v1::ResponseType::Samples),
            value if value == v1::ResponseType::StreamedXorChunks as i32 => {
                Some(v1::ResponseType::StreamedXorChunks)
            }
            _ => None,
        })
        .ok_or_else(|| RemoteReadError::UnsupportedResponseTypes(accepted.to_vec()))
}


/// Default decompressed-body cap for a `remote_read` request when the caller
/// supplies no cap. It mirrors the distributor's ingest default, so a single
/// `read` request cannot decompress to an unbounded allocation.
pub const DEFAULT_MAX_READ_DECOMPRESSED: ByteSize = mebibytes(32);



pub fn encode_chunked_read_frames(
    response: impl Into<crate::wire::remote_read_pb::v1::ReadResponse>,
) -> impl Iterator<Item = Result<Vec<u8>, RemoteReadError>> {
    use crate::wire::remote_read_pb::v1;
    let response: v1::ReadResponse = response.into();
    response
        .results
        .into_iter()
        .enumerate()
        .flat_map(|query_entry| {
            let query_index = query_entry.0;
            let result = query_entry.1;
            result
                .timeseries
                .into_iter()
                .filter(|series| !series.samples.is_empty() || !series.histograms.is_empty())
                .map(move |series| {
                    let query_index = i64::try_from(query_index)
                        .map_err(|_| RemoteReadError::TooManyQueries(query_index))?;
                    let mut chunks = encode_xor_chunks(&series.samples)?;
                    chunks.extend(encode_histogram_chunks(&series.histograms));
                    chunks.sort_by_key(|chunk| chunk.min_time_ms);
                    let response = v1::ChunkedReadResponse {
                        chunked_series: vec![v1::ChunkedSeries {
                            labels: series.labels,
                            chunks,
                        }],
                        query_index,
                    };
                    Ok(frame(response.encode_to_vec()))
                })
        })
}

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let mut framed = Vec::with_capacity(payload.len() + 14);
    push_uvarint(&mut framed, payload.len() as u64);
    framed.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
    framed.extend(payload);
    framed
}

fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8") | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
}

pub(super) fn encode_histogram_chunks(histograms: &[v1::Histogram]) -> Vec<v1::Chunk> {
    let mut histograms = histograms.to_vec();
    histograms.sort_by_key(|histogram| histogram.timestamp);
    histograms.iter().map(encode_histogram_chunk).collect()
}

fn encode_histogram_chunk(histogram: &v1::Histogram) -> v1::Chunk {
    let is_float = matches!(histogram.count, Some(v1::histogram::Count::CountFloat(_)));
    let mut bits = BitStream::new(1);
    bits.write_byte(reset_header(histogram.reset_hint));
    write_layout(&mut bits, histogram);
    put_varbit_int(&mut bits, histogram.timestamp);
    if is_float {
        let count = match histogram.count {
            Some(v1::histogram::Count::CountFloat(count)) => count,
            _ => 0.0,
        };
        let zero_count = match histogram.zero_count {
            Some(v1::histogram::ZeroCount::ZeroCountFloat(count)) => count,
            _ => 0.0,
        };
        bits.write_bits(count.to_bits(), 64);
        bits.write_bits(zero_count.to_bits(), 64);
        bits.write_bits(histogram.sum.to_bits(), 64);
        for count in histogram
            .positive_counts
            .iter()
            .chain(&histogram.negative_counts)
        {
            bits.write_bits(count.to_bits(), 64);
        }
    } else {
        let count = match histogram.count {
            Some(v1::histogram::Count::CountInt(count)) => count,
            _ => 0,
        };
        let zero_count = match histogram.zero_count {
            Some(v1::histogram::ZeroCount::ZeroCountInt(count)) => count,
            _ => 0,
        };
        put_varbit_uint(&mut bits, count);
        put_varbit_uint(&mut bits, zero_count);
        bits.write_bits(histogram.sum.to_bits(), 64);
        for delta in histogram
            .positive_deltas
            .iter()
            .chain(&histogram.negative_deltas)
        {
            put_varbit_int(&mut bits, *delta);
        }
    }
    v1::Chunk {
        min_time_ms: histogram.timestamp,
        max_time_ms: histogram.timestamp,
        r#type: if is_float {
            v1::chunk::Encoding::FloatHistogram as i32
        } else {
            v1::chunk::Encoding::Histogram as i32
        },
        data: bits.bytes,
    }
}

fn reset_header(reset_hint: i32) -> u8 {
    match v1::histogram::ResetHint::try_from(reset_hint) {
        Ok(v1::histogram::ResetHint::Yes) => 0x80,
        Ok(v1::histogram::ResetHint::No) => 0x40,
        Ok(v1::histogram::ResetHint::Gauge) => 0xc0,
        _ => 0,
    }
}

fn write_layout(bits: &mut BitStream, histogram: &v1::Histogram) {
    if histogram.zero_threshold == 0.0 {
        bits.write_byte(0);
    } else {
        bits.write_byte(0xff);
        bits.write_bits(histogram.zero_threshold.to_bits(), 64);
    }
    put_varbit_int(bits, i64::from(histogram.schema));
    write_spans(bits, &histogram.positive_spans);
    write_spans(bits, &histogram.negative_spans);
    if histogram.schema == -53 {
        put_varbit_uint(
            bits,
            u64::try_from(histogram.custom_values.len()).expect("custom bound count fits in u64"),
        );
        for bound in &histogram.custom_values {
            bits.write_bit(false);
            bits.write_bits(bound.to_bits(), 64);
        }
    }
}

fn write_spans(bits: &mut BitStream, spans: &[v1::BucketSpan]) {
    put_varbit_uint(
        bits,
        u64::try_from(spans.len()).expect("span count fits in u64"),
    );
    for span in spans {
        put_varbit_uint(bits, u64::from(span.length));
        put_varbit_int(bits, i64::from(span.offset));
    }
}

fn put_varbit_int(bits: &mut BitStream, value: i64) {
    if value == 0 {
        bits.write_bit(false);
    } else if fits_signed(value, 3) {
        bits.write_bits(0b0000_0010, 2);
        bits.write_bits(value.cast_unsigned(), 3);
    } else if fits_signed(value, 6) {
        bits.write_bits(0b0000_0110, 3);
        bits.write_bits(value.cast_unsigned(), 6);
    } else if fits_signed(value, 9) {
        bits.write_bits(0b0000_1110, 4);
        bits.write_bits(value.cast_unsigned(), 9);
    } else if fits_signed(value, 12) {
        bits.write_bits(0b0001_1110, 5);
        bits.write_bits(value.cast_unsigned(), 12);
    } else if fits_signed(value, 18) {
        bits.write_bits(0b0011_1110, 6);
        bits.write_bits(value.cast_unsigned(), 18);
    } else if fits_signed(value, 25) {
        bits.write_bits(0b0111_1110, 7);
        bits.write_bits(value.cast_unsigned(), 25);
    } else if fits_signed(value, 56) {
        bits.write_bits(0b1111_1110, 8);
        bits.write_bits(value.cast_unsigned(), 56);
    } else {
        bits.write_bits(0xff, 8);
        bits.write_bits(value.cast_unsigned(), 64);
    }
}

fn put_varbit_uint(bits: &mut BitStream, value: u64) {
    if value == 0 {
        bits.write_bit(false);
    } else if fits_unsigned(value, 3) {
        bits.write_bits(0b0000_0010, 2);
        bits.write_bits(value, 3);
    } else if fits_unsigned(value, 6) {
        bits.write_bits(0b0000_0110, 3);
        bits.write_bits(value, 6);
    } else if fits_unsigned(value, 9) {
        bits.write_bits(0b0000_1110, 4);
        bits.write_bits(value, 9);
    } else if fits_unsigned(value, 12) {
        bits.write_bits(0b0001_1110, 5);
        bits.write_bits(value, 12);
    } else if fits_unsigned(value, 18) {
        bits.write_bits(0b0011_1110, 6);
        bits.write_bits(value, 18);
    } else if fits_unsigned(value, 25) {
        bits.write_bits(0b0111_1110, 7);
        bits.write_bits(value, 25);
    } else if fits_unsigned(value, 56) {
        bits.write_bits(0b1111_1110, 8);
        bits.write_bits(value, 56);
    } else {
        bits.write_bits(0xff, 8);
        bits.write_bits(value, 64);
    }
}


fn fits_unsigned(value: u64, width: u32) -> bool {
    value.leading_zeros() >= 64 - width
}

const MAX_SAMPLES_PER_CHUNK: usize = 120;

pub(crate) fn encode_xor_chunks(samples: &[v1::Sample]) -> Result<Vec<v1::Chunk>, RemoteReadError> {
    let mut samples = samples.to_vec();
    samples.sort_by_key(|sample| sample.timestamp);
    samples
        .chunks(MAX_SAMPLES_PER_CHUNK)
        .map(encode_xor_chunk)
        .collect()
}

fn encode_xor_chunk(samples: &[v1::Sample]) -> Result<v1::Chunk, RemoteReadError> {
    let count = u16::try_from(samples.len()).expect("XOR chunks are capped at 120 samples");
    let mut bits = BitStream::new(count);
    let mut timestamp = i64::MIN;
    let mut value = 0.0;
    let mut timestamp_delta = 0_u64;
    let mut leading = u8::MAX;
    let mut trailing = 0_u8;

    for sample_entry in samples.iter().enumerate() {
        let index = sample_entry.0;
        let sample = sample_entry.1;
        match index {
            0 => {
                bits.write_varint(sample.timestamp);
                bits.write_bits(sample.value.to_bits(), 64);
            }
            1 => {
                timestamp_delta = u64::try_from(
                    i128::from(sample.timestamp) - i128::from(timestamp),
                )
                .map_err(|_| RemoteReadError::UnorderedSamples {
                    previous: timestamp,
                    current: sample.timestamp,
                })?;
                bits.write_uvarint(timestamp_delta);
                write_value_delta(&mut bits, sample.value, value, &mut leading, &mut trailing);
            }
            _ => {
                let next_delta = u64::try_from(
                    i128::from(sample.timestamp) - i128::from(timestamp),
                )
                .map_err(|_| RemoteReadError::UnorderedSamples {
                    previous: timestamp,
                    current: sample.timestamp,
                })?;
                write_timestamp_delta_of_delta(
                    &mut bits,
                    next_delta.wrapping_sub(timestamp_delta).cast_signed(),
                );
                timestamp_delta = next_delta;
                write_value_delta(&mut bits, sample.value, value, &mut leading, &mut trailing);
            }
        }
        timestamp = sample.timestamp;
        value = sample.value;
    }

    Ok(v1::Chunk {
        min_time_ms: samples.first().map_or(0, |sample| sample.timestamp),
        max_time_ms: samples.last().map_or(0, |sample| sample.timestamp),
        r#type: v1::chunk::Encoding::Xor as i32,
        data: bits.bytes,
    })
}

fn write_timestamp_delta_of_delta(bits: &mut BitStream, delta: i64) {
    if delta == 0 {
        bits.write_bit(false);
    } else if fits_signed(delta, 14) {
        bits.write_bits(0b10, 2);
        bits.write_bits(delta.cast_unsigned(), 14);
    } else if fits_signed(delta, 17) {
        bits.write_bits(0b110, 3);
        bits.write_bits(delta.cast_unsigned(), 17);
    } else if fits_signed(delta, 20) {
        bits.write_bits(0b1110, 4);
        bits.write_bits(delta.cast_unsigned(), 20);
    } else {
        bits.write_bits(0b1111, 4);
        bits.write_bits(delta.cast_unsigned(), 64);
    }
}

fn fits_signed(value: i64, width: u32) -> bool {
    -((1_i64 << (width - 1)) - 1) <= value && value <= 1_i64 << (width - 1)
}

fn write_value_delta(
    bits: &mut BitStream,
    next: f64,
    current: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = next.to_bits() ^ current.to_bits();
    if delta == 0 {
        bits.write_bit(false);
        return;
    }
    bits.write_bit(true);

    let next_leading = u8::try_from(delta.leading_zeros()).unwrap().min(31);
    let next_trailing = u8::try_from(delta.trailing_zeros()).unwrap();
    if *leading != u8::MAX && next_leading >= *leading && next_trailing >= *trailing {
        bits.write_bit(false);
        bits.write_bits(
            delta >> *trailing,
            64 - u32::from(*leading) - u32::from(*trailing),
        );
        return;
    }

    *leading = next_leading;
    *trailing = next_trailing;
    bits.write_bit(true);
    bits.write_bits(u64::from(next_leading), 5);
    let significant = 64 - u32::from(next_leading) - u32::from(next_trailing);
    bits.write_bits(u64::from(significant & 0x3f), 6);
    bits.write_bits(delta >> next_trailing, significant);
}

pub(super) struct BitStream {
    pub(super) bytes: Vec<u8>,
    available: u8,
}

impl BitStream {
    pub(super) fn new(sample_count: u16) -> Self {
        Self {
            bytes: sample_count.to_be_bytes().to_vec(),
            available: 0,
        }
    }

    pub(super) fn write_bit(&mut self, value: bool) {
        if self.available == 0 {
            self.bytes.push(0);
            self.available = 8;
        }
        if value {
            let last = self
                .bytes
                .last_mut()
                .expect("bit stream has a current byte");
            *last |= 1 << (self.available - 1);
        }
        self.available -= 1;
    }

    pub(super) fn write_byte(&mut self, value: u8) {
        if self.available == 0 {
            self.bytes.push(value);
            return;
        }
        let last = self
            .bytes
            .last_mut()
            .expect("bit stream has a current byte");
        *last |= value >> (8 - self.available);
        self.bytes.push(value << self.available);
    }

    pub(super) fn write_bits(&mut self, value: u64, width: u32) {
        let shifted = if width == 64 {
            value
        } else {
            value << (64 - width)
        };
        let mut remaining = width;
        let mut shifted = shifted;
        while remaining >= 8 {
            self.write_byte((shifted >> 56) as u8);
            shifted <<= 8;
            remaining -= 8;
        }
        while remaining > 0 {
            self.write_bit(shifted >> 63 == 1);
            shifted <<= 1;
            remaining -= 1;
        }
    }

    fn write_uvarint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.write_byte(
                u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8") | 0x80,
            );
            value >>= 7;
        }
        self.write_byte(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
    }

    fn write_varint(&mut self, value: i64) {
        let mut encoded = value.cast_unsigned() << 1;
        if value < 0 {
            encoded = !encoded;
        }
        self.write_uvarint(encoded);
    }
}

/// The matcher set and inclusive time range of one remote read query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteReadSelectors {
    pub selectors: Vec<LabelMatcher>,
    pub start_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn matchers_to_selectors(query: &v1::Query) -> Result<RemoteReadSelectors, RemoteReadError> {
    let selectors = query
        .matchers
        .iter()
        .map(|matcher| {
            let op =
                MatchOp::try_from(matcher.r#type).map_err(RemoteReadError::UnsupportedMatcher)?;
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RemoteReadSelectors {
        selectors,
        start_timestamp_ms: query.start_timestamp_ms,
        end_timestamp_ms: query.end_timestamp_ms,
    })
}

#[derive(Debug, Error)]
pub enum RemoteReadError {
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy encode failed: {0}")]
    SnappyEncode(String),
    #[error("protobuf decode failed: {0}")]
    Decode(String),
    #[error("protobuf encode failed: {0}")]
    Encode(String),
    #[error("unsupported remote_read matcher type {0}")]
    UnsupportedMatcher(i32),
    #[error("server does not support any requested remote_read response type: {0:?}")]
    UnsupportedResponseTypes(Vec<i32>),
    #[error("remote_read samples are out of timestamp order: {previous} then {current}")]
    UnorderedSamples { previous: i64, current: i64 },
    #[error("remote_read query count exceeds i64 at index {0}")]
    TooManyQueries(usize),
}

/// One float sample in a remote read series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RemoteReadSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A label set and its float samples for a remote read response.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteReadSeries {
    pub labels: Labels,
    pub samples: Vec<RemoteReadSample>,
}

fn compare_labels(left: &[v1::Label], right: &[v1::Label]) -> std::cmp::Ordering {
    for index in 0..left.len().min(right.len()) {
        let ordering = left[index]
            .name
            .cmp(&right[index].name)
            .then_with(|| left[index].value.cmp(&right[index].value));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

#[must_use]
pub fn series_to_timeseries(series: Vec<RemoteReadSeries>) -> v1::QueryResult {
    let mut timeseries = series
        .into_iter()
        .map(|series| {
            let labels = series.labels;
            let mut labels = labels
                .iter()
                .map(|pair| v1::Label {
                    name: pair.name.to_owned(),
                    value: pair.value.to_owned(),
                })
                .collect::<Vec<_>>();
            labels.sort_by(|left, right| left.name.cmp(&right.name));

            let mut samples = series
                .samples
                .into_iter()
                .map(|sample| v1::Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_ms,
                })
                .collect::<Vec<_>>();
            samples.sort_by_key(|sample| sample.timestamp);

            v1::TimeSeries {
                labels,
                samples,
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    timeseries.sort_by(|left, right| compare_labels(&left.labels, &right.labels));
    v1::QueryResult { timeseries }
}
