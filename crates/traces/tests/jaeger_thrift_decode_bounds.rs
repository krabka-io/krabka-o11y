//! A malformed Jaeger Thrift payload must fail fast rather than spin.
//!
//! The compact decoder sits behind `handle_jaeger_compact_datagram`, which the
//! UDP receiver calls inline for every datagram it takes off the socket:
//! unauthenticated, unsolicited, and trivially spoofable. A decode that does
//! not return therefore pins the receiving worker thread for the life of the
//! process, so each case here is run under a wall clock and fails rather than
//! hangs when the bound comes back.

use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use assert2::check;
use krabka_traces::{
    Span, TracesError,
    wire::jaeger::{decode_jaeger_binary_thrift, decode_jaeger_thrift},
};

/// The wall clock a decode of a short payload is allowed.
///
/// Generous by three orders of magnitude: every case here is microseconds of
/// work, and the bound only has to be small enough that the unbounded loops
/// this file guards against -- 7.8 seconds on the binary reproducer, and no
/// return at all on the compact one -- cannot slip under it on a loaded
/// runner.
const DECODE_BUDGET: Duration = Duration::from_secs(20);

/// Decode `body` on a thread of its own and give up on it after
/// [`DECODE_BUDGET`], returning the result and what it took.
///
/// A regression does not return, so calling the decoder directly would hang
/// the suite instead of failing it. The worker is left to spin; the harness
/// ends the process when the run does.
fn decode_within(
    decode: fn(&[u8]) -> Result<Vec<Span>, TracesError>,
    body: Vec<u8>,
) -> (Result<Vec<Span>, TracesError>, Duration) {
    let (results, decoded) = mpsc::channel();
    let started = Instant::now();
    thread::Builder::new()
        .name("jaeger-decode".to_owned())
        .spawn(move || {
            let _ = results.send(decode(&body));
        })
        .expect("spawns a decode thread");

    let Ok(result) = decoded.recv_timeout(DECODE_BUDGET) else {
        panic!("the decode did not return within {DECODE_BUDGET:?}")
    };
    (result, started.elapsed())
}

/// Eleven bytes off the fuzzer. The list header declares 2^60 boolean
/// elements, and a compact boolean carries its value in the type nibble of a
/// field header -- so a skip that treats an element like a field reads
/// nothing per iteration, runs out of neither input nor patience, and spins
/// for what at 2.5e8 iterations a second is a span of millennia.
#[test]
fn the_compact_datagram_that_declared_a_quintillion_booleans_is_refused() {
    let body = vec![
        // A list in field 1, which the batch reads as neither its process
        // nor its span list, and therefore skips.
        0x19, // Booleans, with the length in the varint that follows.
        0xF1, // 2^60, in nine bytes of varint.
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10,
    ];

    let (result, elapsed) = decode_within(decode_jaeger_thrift, body);

    check!(let Err(TracesError::Decode(_)) = &result);
    check!(
        elapsed < DECODE_BUDGET,
        "eleven bytes are refused in the time it takes to read them"
    );
}

/// Eight bytes off the fuzzer, and the binary protocol's form of the same
/// defect: the stop type is the one element type whose skip reads nothing, so
/// an i32 length of 2^31 - 1 of them burned 7.8 seconds of a core per packet.
#[test]
fn the_binary_body_that_declared_two_billion_stop_elements_is_refused() {
    let body = vec![
        // A list in field 1, which the batch skips.
        0x0F, 0x00, 0x01, // Elements of the stop type.
        0x00, // 2^31 - 1 of them.
        0x7F, 0xFF, 0xFF, 0xFF,
    ];

    let (result, elapsed) = decode_within(decode_jaeger_binary_thrift, body);

    check!(let Err(TracesError::Decode(_)) = &result);
    check!(
        elapsed < DECODE_BUDGET,
        "eight bytes are refused in the time it takes to read them"
    );
}

/// The other dimension of the same skip: nesting rather than length. Every
/// byte of a compact list-of-lists opens another level, and each level is a
/// stack frame, so a datagram that fills the receiver's 64 KiB buffer would
/// recurse tens of thousands deep. Depth is capped, and the payload is
/// refused as a decode error rather than as a stack overflow -- which is a
/// killed process, not a rejected packet.
#[test]
fn a_datagram_of_nothing_but_nested_compact_lists_is_refused() {
    // The first byte is the field header; each byte after it is a
    // one-element list whose element is another list.
    let body = vec![0x19; 65_535];

    let (result, elapsed) = decode_within(decode_jaeger_thrift, body);

    check!(let Err(TracesError::Decode(_)) = &result);
    check!(elapsed < DECODE_BUDGET);
}

/// The binary protocol's nesting is five bytes a level rather than one, and
/// is bounded the same way.
#[test]
fn a_body_of_nothing_but_nested_binary_lists_is_refused() {
    let mut body = vec![0x0F, 0x00, 0x01];
    for _ in 0..13_000 {
        body.extend_from_slice(&[0x0F, 0x00, 0x00, 0x00, 0x01]);
    }

    let (result, elapsed) = decode_within(decode_jaeger_binary_thrift, body);

    check!(let Err(TracesError::Decode(_)) = &result);
    check!(elapsed < DECODE_BUDGET);
}
