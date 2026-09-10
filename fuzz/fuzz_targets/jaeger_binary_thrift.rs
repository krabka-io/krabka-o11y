//! The Jaeger binary-Thrift decoder, on raw bytes.
//!
//! The binary protocol frames a length as a big-endian i32 rather than a
//! varint, so it can express a negative one. The compact protocol has no way
//! to write that shape, and this decoder has to refuse it on its own.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_traces::wire::jaeger::decode_jaeger_binary_thrift(data);
});
