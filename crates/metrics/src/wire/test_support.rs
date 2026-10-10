//! Helpers the wire decoder tests share.

use assert2::check;

use super::{DecodedSample, DecodedSeries};

pub fn snappy(body: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(body).unwrap()
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
