//! The PromQL parser, on arbitrary text.
//!
//! A query string arrives from `/api/v1/query` and friends before anything has
//! authenticated it, and the parser is the first thing that reads it.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|query: &str| {
    let _ = krabka_promql::parse_promql(query);
});
