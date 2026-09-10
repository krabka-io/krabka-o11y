//! Prometheus `remote_write` v1 ingest, on bodies that decompress and parse.
//!
//! Past snappy and past prost lies the part written here: label validation,
//! and the native-histogram conversion whose bucket spans, delta runs and
//! count runs have to agree on their lengths. The model picks each of those
//! independently, so most draws disagree.

#![no_main]

use krabka_o11y_fuzz::remote_write::{WriteRequestModel, encode_v1};
use krabka_units::prelude::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|model: WriteRequestModel| {
    let _ = krabka_metrics::wire::decode_v1(&encode_v1(&model), mebibytes(1));
});
