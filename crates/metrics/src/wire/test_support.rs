//! Helpers the v1 and v2 decoder tests share.

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
