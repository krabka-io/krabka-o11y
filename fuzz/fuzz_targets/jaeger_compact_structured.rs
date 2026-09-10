//! The Jaeger compact-Thrift decoder, on bodies that are framed correctly.
//!
//! The raw-bytes target beside this one is stopped by the first length check
//! far more often than it gets past it. This one builds a `Batch` whose
//! framing holds together and whose field ids, wire types and values the
//! fuzzer chose, so the run reaches the struct-list sizing, the `skip` arm and
//! the span conversion rather than re-deriving the varint format.

#![no_main]

use krabka_o11y_fuzz::thrift::{BatchModel, encode_batch};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|model: BatchModel| {
    let _ = krabka_traces::wire::jaeger::decode_jaeger_thrift(&encode_batch(&model));
});
