//! A populated in-memory span store, for `TraceQL` filtering.

use krabka_traceql::{AttrValue, InMemorySpanStore, InputSpan};
use krabka_units::{Time, convert::TimeExt as _};

use crate::{Seeded, index::TENANT};

/// Distinct service names across the fixture. A `.service.name` matcher then
/// selects a known fraction of the spans rather than all or one of them.
const SERVICES: usize = 32;
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const STATUSES: [i64; 4] = [200, 404, 500, 503];

/// The nanosecond timestamp the fixture's first span starts at.
pub const START_NS: i64 = 1_000_000_000;

/// A store holding `traces` traces of `spans_per_trace` spans each.
///
/// Every trace is a root with a flat fan of children, which is the shape that
/// makes the span count the parameter under test: a deep tree would make the
/// structural operators the thing being measured, and those are a different
/// hot path with a different curve.
///
/// # Panics
/// Panics when `spans_per_trace` is zero, which is a trace with no root.
#[must_use]
pub fn span_store(traces: usize, spans_per_trace: usize) -> InMemorySpanStore {
    assert!(spans_per_trace > 0, "a trace needs at least a root span");
    let mut store = InMemorySpanStore::new();
    let mut pick = Seeded::new(0x_5A11_5EED);

    for trace in 0..traces {
        let trace_id = trace_bytes(trace);
        let mut spans = Vec::with_capacity(spans_per_trace);
        for index in 0..spans_per_trace {
            let service = format!("svc-{}", pick.next_below(SERVICES));
            let parent = (index > 0).then(|| span_bytes(0));
            spans.push(InputSpan {
                trace_id,
                span_id: span_bytes(index),
                parent_span_id: parent,
                name: if index == 0 {
                    "root".to_string()
                } else {
                    format!("child-{index}")
                },
                kind: 0,
                start_unix_nano: START_NS
                    + i64::try_from(index).expect("a span index fits an i64") * 1_000,
                duration: Time::from_nanos(
                    100_000 + i64::try_from(pick.next_below(900_000)).expect("a bound fits an i64"),
                ),
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                attrs: vec![
                    ("service.name".to_string(), AttrValue::Str(service)),
                    (
                        "http.method".to_string(),
                        AttrValue::Str(METHODS[pick.next_below(METHODS.len())].to_string()),
                    ),
                    (
                        "http.status_code".to_string(),
                        AttrValue::Int(STATUSES[pick.next_below(STATUSES.len())]),
                    ),
                ],
                events: Vec::new(),
                links: Vec::new(),
            });
        }
        store.push_trace(TENANT, "svc-0", "root", spans);
    }
    store
}

/// The last timestamp a store built with `spans_per_trace` spans carries,
/// with room past it so a query's window is not a boundary case.
///
/// # Panics
/// Panics when a span count does not fit an `i64`.
#[must_use]
pub fn end_ns(spans_per_trace: usize) -> i64 {
    START_NS + i64::try_from(spans_per_trace).expect("a span count fits an i64") * 1_000 + 1_000_000
}

/// The 16-byte id of trace `trace`.
fn trace_bytes(trace: usize) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id[8..].copy_from_slice(
        &u64::try_from(trace)
            .expect("a trace index fits a u64")
            .to_be_bytes(),
    );
    // A trace id of all zeroes is a valid `[u8; 16]` and an invalid trace id;
    // the high half keeps every generated id non-zero.
    id[..8].copy_from_slice(&0x7ACE_u64.to_be_bytes());
    id
}

/// The 8-byte id of span `index` within its trace.
fn span_bytes(index: usize) -> [u8; 8] {
    (u64::try_from(index).expect("a span index fits a u64") + 1).to_be_bytes()
}
