//! The Prometheus remote-read request decoder.
//!
//! Same snappy-over-protobuf framing as remote-write, a different message, and
//! its own decompression bound.

#![no_main]

use krabka_metrics::wire::{DEFAULT_MAX_READ_DECOMPRESSED, decode_read_request};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_read_request(data, DEFAULT_MAX_READ_DECOMPRESSED);
});
