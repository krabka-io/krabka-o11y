//! Shared decode target, content negotiation, snappy-block decode, and
//! `remote_write` status mapping.

use krabka_blockstore::Labels;
use krabka_units::prelude::*;

use crate::NativeHistogram;

#[cfg(test)]
mod tests {

    /// A decoded sample compares equal to a `(timestamp, value)` pair only
    /// when it carries no start timestamp. Three conditions have to hold, so
    /// each case below fails exactly one of them: with two failing at once,
    /// joining them by `or` instead of `and` would still answer false and the
    /// mutant would live.
    #[test]
    fn a_sample_equals_a_pair_only_without_a_start_timestamp() {
        use super::DecodedSample;

        let sample = DecodedSample::new(100, 1.5);
        check!(sample == (100, 1.5), "everything matches");

        // Each condition failing on its own.
        check!(!(sample == (101, 1.5)), "the timestamp differs");
        check!(!(sample == (100, 2.5)), "the value differs");
        let with_start = DecodedSample {
            timestamp_ms: 100,
            value: 1.5,
            start_timestamp_ms: Some(50),
        };
        check!(
            !(with_start == (100, 1.5)),
            "a start timestamp makes it a different thing from a bare pair"
        );

        // A start timestamp of zero is still a start timestamp, not an absence.
        let zero_start = DecodedSample {
            timestamp_ms: 100,
            value: 1.5,
            start_timestamp_ms: Some(0),
        };
        check!(!(zero_start == (100, 1.5)));
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
    fn negotiate_rejects_json() {
        let err = negotiate(Some("application/json")).unwrap_err();
        assert!(matches!(err, WireError::UnsupportedContentType(_)));
        assert!(err.status_code() == 415);
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
        // Hand-roll a raw snappy block: a varint preamble declaring ~1 GiB of
        // output followed by a one-byte literal. `decompress_len` reads the
        // preamble; the guard fires without ever allocating the gigabyte.
        let huge: u64 = 1 << 30;
        let mut frame = Vec::new();
        let mut value = huge;
        while value >= 0x80 {
            frame.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        frame.push(u8::try_from(value).unwrap());
        // One literal byte (tag 0x00 = literal, length-1 encoded in upper bits).
        frame.push(0x00);
        frame.push(0x42);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = snappy_block_decode(&frame, mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::SnappyOutputTooLarge(_)));
        assert!(err.status_code() == 400);
    }
}

// === split-modules: generated submodules ===
mod decoded_exemplar;
mod decoded_metadata;
mod decoded_sample;
mod decoded_series;
mod negotiate;
mod proto_param_value;
mod snappy_block_decode;
mod snappy_block_decode_raw;
mod wire_error;
mod wire_format;

pub use decoded_exemplar::DecodedExemplar;
pub use decoded_metadata::DecodedMetadata;
pub use decoded_sample::DecodedSample;
pub use decoded_series::DecodedSeries;
pub use negotiate::negotiate;
use proto_param_value::proto_param_value;
# [cfg_attr (test , mutants :: skip)] pub use snappy_block_decode::snappy_block_decode;
# [cfg_attr (test , mutants :: skip)] pub (super) use snappy_block_decode_raw::snappy_block_decode_raw;
pub use wire_error::WireError;
pub use wire_format::WireFormat;
