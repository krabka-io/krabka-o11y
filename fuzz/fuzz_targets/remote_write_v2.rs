//! Prometheus `remote_write` v2 ingest.
//!
//! v2 replaces the repeated label strings of v1 with references into a symbol
//! table the same body carries, so every label read is an index an attacker
//! chose into an array whose length they also chose.

#![no_main]

use krabka_units::prelude::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = krabka_metrics::wire::decode_v2(data, mebibytes(1));
});
