//! The Jaeger compact-Thrift decoder, on raw bytes.
//!
//! This is the widest unauthenticated surface in the repository. The same
//! decoder serves `/api/traces` over HTTP and the Jaeger agent's UDP port, and
//! a datagram handler gets attacker-chosen bytes with no connection, no
//! handshake and no length the sender had to commit to first. The property is
//! the one //docs/style_guides/code_style_guide.md states: malformed wire
//! input returns an error, and never a panic.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_traces::wire::jaeger::decode_jaeger_thrift(data);
});
