//! OTLP trace ingest.
//!
//! OTLP is the path a modern tracer takes, so `decode_otlp` reads more
//! attacker-chosen bytes in practice than the Jaeger doors do. It takes a
//! decoded `TracesData` rather than a body, so the target decodes one first:
//! what it covers is the conversion to internal spans, which is where the
//! fixed-width trace and span ids and the timestamp arithmetic live.

#![no_main]

use libfuzzer_sys::fuzz_target;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    if let Ok(traces) = TracesData::decode(data) {
        let _ = krabka_traces::wire::otlp::decode_otlp(&traces);
    }
});
