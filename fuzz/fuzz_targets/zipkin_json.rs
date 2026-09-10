//! The Zipkin v2 JSON span decoder.
//!
//! serde_json does the parsing, so what this exercises is what the decoder
//! does afterwards: the fixed-width hex ids, and the microsecond-to-nanosecond
//! scaling of a timestamp and a duration the sender chose.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_traces::wire::zipkin::decode_zipkin(data);
});
