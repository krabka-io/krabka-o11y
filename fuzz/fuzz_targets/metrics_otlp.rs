//! OTLP metrics ingest.
//!
//! `MetricsData` carries every OTLP point type, and the translation to
//! Prometheus series is where the exponential-histogram scale and the
//! delta-to-cumulative bookkeeping happen.

#![no_main]

use krabka_metrics::otlp::{TranslationStrategy, decode_otlp_bytes};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_otlp_bytes(data, TranslationStrategy::UnderscoreEscapingWithSuffixes);
});
