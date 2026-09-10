//! Prometheus `remote_write` v1 ingest, on raw bytes.
//!
//! The body is snappy over protobuf, so this target covers the decompression
//! bound as well as the decode: the snappy frame names its own output size,
//! and `max_decompressed` is what keeps a small body from asking for a large
//! allocation.

#![no_main]

use krabka_units::prelude::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_metrics::wire::decode_v1(data, mebibytes(1));
});
