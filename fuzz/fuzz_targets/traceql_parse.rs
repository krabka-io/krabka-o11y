//! The TraceQL parser, on arbitrary text.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|query: &str| {
    let _ = krabka_traceql::parse(query);
});
