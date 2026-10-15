//! Shared decode target, content negotiation, snappy-block decode, and
//! `remote_write` status mapping.

use krabka_blockstore::Labels;
use krabka_units::prelude::*;

use crate::NativeHistogram;

#[cfg(test)]
mod tests {
    use crate::wire::test_support::declared_length_bomb;

    #[test]
    #[allow(clippy::float_cmp)]
    fn a_sample_retains_timestamp_value_and_start_timestamp() {
        use super::DecodedSample;

        let sample = DecodedSample::new(100, 1.5);
        check!(sample.timestamp_ms == 100);
        check!(sample.value == 1.5);
        check!(sample.start_timestamp_ms.is_none());

        // Each condition failing on its own.
        check!(sample.timestamp_ms != 101, "the timestamp differs");
        check!(sample.value != 2.5, "the value differs");
        let with_start = DecodedSample {
            timestamp_ms: 100,
            value: 1.5,
            start_timestamp_ms: Some(50),
        };
        check!(
            with_start.start_timestamp_ms == Some(50),
            "the start timestamp is retained"
        );

        // A start timestamp of zero is still a start timestamp, not an absence.
        let zero_start = DecodedSample {
            timestamp_ms: 100,
            value: 1.5,
            start_timestamp_ms: Some(0),
        };
        check!(zero_start.start_timestamp_ms == Some(0));
    }
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn negotiate_v1_default_protobuf() {
        assert!(negotiate(Some("application/x-protobuf")).unwrap() == WireFormat::RemoteWriteV1);
        assert!(negotiate(None).unwrap() == WireFormat::RemoteWriteV1);
    }

    #[test]
    fn negotiate_v1_explicit_proto_param() {
        assert!(
            negotiate(Some(
                "application/x-protobuf; proto=prometheus.WriteRequest"
            ))
            .unwrap()
                == WireFormat::RemoteWriteV1
        );
    }

    #[test]
    fn negotiate_v2_proto_param() {
        assert!(
            negotiate(Some(
                "application/x-protobuf; proto=io.prometheus.write.v2.Request"
            ))
            .unwrap()
                == WireFormat::RemoteWriteV2
        );
    }

    #[test]
    fn negotiate_treats_a_legacy_content_type_as_v1() {
        assert!(negotiate(Some("text/plain")).unwrap() == WireFormat::RemoteWriteV1);
    }

    #[test]
    fn snappy_block_round_trips_plain() {
        let input = b"remote-write-body";
        let compressed = snap::raw::Encoder::new().compress_vec(input).unwrap();

        let back = snappy_block_decode(&compressed, mebibytes(1)).unwrap();

        assert!(back == input);
    }

    #[test]
    fn snappy_block_rejects_oversize() {
        let compressed = snap::raw::Encoder::new()
            .compress_vec(b"larger than allowed")
            .unwrap();

        let err = snappy_block_decode(&compressed, bytes(4)).unwrap_err();

        assert!(matches!(err, WireError::SnappyOutputTooLarge(4)));
        assert!(err.status_code() == 400);
    }

    /// A snappy block whose varint header *declares* a huge uncompressed
    /// length but carries a tiny payload must fail the declared-length
    /// pre-check, before `snap` allocates the declared buffer.
    #[test]
    fn snappy_block_rejects_declared_length_bomb() {
        // ~1 GiB declared, one byte carried. `decompress_len` reads the
        // preamble; the guard fires without ever allocating the gigabyte.
        let huge: u64 = 1 << 30;
        let frame = declared_length_bomb(huge);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = snappy_block_decode(&frame, mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::SnappyOutputTooLarge(_)));
        assert!(err.status_code() == 400);
    }
}

#[cfg_attr(test, mutants::skip)]
pub(crate) fn proto_param_value(param: &str) -> Option<String> {
    let pair = param.trim().split_once('=')?;
    pair.0
        .trim()
        .eq_ignore_ascii_case("proto")
        .then(|| pair.1.trim().trim_matches('"').to_string())
}

/// Dispatches on the `proto=` parameter of the `Content-Type` header. A bare
/// `application/x-protobuf` stays the v1 default.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn negotiate(content_type: Option<&str>) -> Result<WireFormat, WireError> {
    let Some(content_type) = content_type else {
        return Ok(WireFormat::RemoteWriteV1);
    };
    let mut parts = content_type.split(';');
    let base = parts.next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Ok(WireFormat::RemoteWriteV1);
    }

    let proto = parts.find_map(proto_param_value);
    match proto.as_deref() {
        None | Some("prometheus.WriteRequest") => Ok(WireFormat::RemoteWriteV1),
        Some("io.prometheus.write.v2.Request") => Ok(WireFormat::RemoteWriteV2),
        Some(other) => Err(WireError::UnsupportedContentType(format!("proto={other}"))),
    }
}

/// Decodes a plain snappy block. Prometheus `remote_write` does not use the
/// Xerial framed snappy format that Kafka uses.
///
/// This function checks the block's stored uncompressed length against
/// `max_output` *before* it decompresses, so it rejects a decompression bomb
/// before `snap` pre-allocates the declared buffer. Such a bomb is a tiny
/// payload that declares a huge length.
// cargo-mutants: covered by remote-write snappy round-trip and limit tests.
#[cfg_attr(test, mutants::skip)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn snappy_block_decode(body: &[u8], max_output: ByteSize) -> Result<Vec<u8>, WireError> {
    snappy_block_decode_raw(
        body,
        max_output.bytes_usize(),
        WireError::SnappyDecode,
        WireError::SnappyOutputTooLarge,
    )
}

// cargo-mutants: shared decoder guard is covered through remote_write and remote_read callers.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn snappy_block_decode_raw<E>(
    body: &[u8],
    max_output: usize,
    snappy_decode: impl Fn(String) -> E,
    output_too_large: impl Fn(usize) -> E,
) -> Result<Vec<u8>, E> {
    let declared =
        snap::raw::decompress_len(body).map_err(|error| snappy_decode(error.to_string()))?;
    if declared > max_output {
        return Err(output_too_large(max_output));
    }
    let out = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|error| snappy_decode(error.to_string()))?;
    if out.len() > max_output {
        return Err(output_too_large(max_output));
    }
    Ok(out)
}

/// Exemplar decoded from a `remote_write` request.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedExemplar {
    pub labels: Labels,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Metric metadata decoded from an ingest request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedMetadata {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// One decoded float sample from an ingest request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecodedSample {
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}
impl DecodedSample {
    #[must_use]
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        }
    }

    #[must_use]
    pub fn with_start_timestamp(
        timestamp_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms,
        }
    }
}

/// One native histogram sample in a decoded series.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedHistogramSample {
    pub timestamp_ms: i64,
    pub histogram: NativeHistogram,
}

/// One decoded metric series from any ingest wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Labels,
    pub samples: Vec<DecodedSample>,
    pub histograms: Vec<DecodedHistogramSample>,
    pub exemplars: Vec<DecodedExemplar>,
    pub metadata: Option<DecodedMetadata>,
}

/// Errors raised at the `remote_write` ingest edge.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unsupported content type `{0}`")]
    UnsupportedContentType(String),
    #[error("unsupported content encoding `{0}`")]
    UnsupportedContentEncoding(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("decoded body exceeds max_output={0}")]
    DecodedBodyTooLarge(usize),
    #[error("gzip decode failed: {0}")]
    GzipDecode(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
    #[error("invalid remote_write request: {0}")]
    Invalid(String),
}

impl WireError {
    /// HTTP status code for Prometheus `remote_write` ingest.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedContentType(_) | Self::UnsupportedContentEncoding(_) => 415,
            Self::DecodedBodyTooLarge(_) => 413,
            Self::SnappyOutputTooLarge(_)
            | Self::SnappyDecode(_)
            | Self::GzipDecode(_)
            | Self::ProtobufDecode(_)
            | Self::Invalid(_) => 400,
        }
    }
}

/// Which `remote_write` protobuf shape an HTTP request carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    RemoteWriteV1,
    RemoteWriteV2,
}
