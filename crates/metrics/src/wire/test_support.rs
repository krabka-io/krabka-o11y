//! Helpers the wire decoder tests share.

use assert2::check;

use super::{DecodedSample, DecodedSeries, pb};

pub fn snappy(body: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(body).unwrap()
}

/// A v1 series named `up` that holds only `sample`.
pub fn up_v1_series(sample: pb::v1::Sample) -> pb::v1::TimeSeries {
    pb::v1::TimeSeries {
        labels: vec![pb::v1::Label {
            name: "__name__".into(),
            value: "up".into(),
        }],
        samples: vec![sample],
        ..Default::default()
    }
}

/// A v2 request whose one series is named `up` and holds only `sample`.
pub fn up_v2_request(sample: pb::v2::Sample) -> pb::v2::Request {
    pb::v2::Request {
        symbols: vec![String::new(), "__name__".into(), "up".into()],
        timeseries: vec![pb::v2::TimeSeries {
            labels_refs: vec![1, 2],
            samples: vec![sample],
            ..Default::default()
        }],
    }
}

// Checks the one series both decoders' basic case writes: `up` at 1000 with
// value 1.0, and an exemplar carrying `trace_id`.
pub fn check_up_sample_with_trace_exemplar(decoded: &[DecodedSeries]) {
    assert2::assert!(decoded.len() == 1);
    check!(decoded[0].labels.get("__name__") == Some("up"));
    check!(decoded[0].samples == vec![DecodedSample::new(1000, 1.0)]);
    check!(decoded[0].exemplars[0].labels.get("trace_id") == Some("abc"));
}

// A raw snappy block whose varint preamble declares `declared_len` bytes of
// output but whose payload is a single literal byte, so only a declared-length
// pre-check stops a decoder from allocating the declared buffer.
pub fn declared_length_bomb(declared_len: u64) -> Vec<u8> {
    let mut frame = Vec::new();
    let mut value = declared_len;
    while value >= 0x80 {
        frame.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
        value >>= 7;
    }
    frame.push(u8::try_from(value).unwrap());
    // One literal byte (tag 0x00 = literal, length-1 encoded in upper bits).
    frame.push(0x00);
    frame.push(0x42);
    frame
}
