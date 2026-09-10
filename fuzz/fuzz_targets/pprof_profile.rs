//! The pprof profile decoder.
//!
//! `PprofProfile::decode` is the door every Pyroscope-compatible ingest path
//! goes through, including the gzip-wrapped bodies of `push.v1`.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_pprof::PprofProfile::decode(data);
});
