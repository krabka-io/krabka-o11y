//! Tempo-compatible traces service for Krabka.

#![forbid(unsafe_code)]

use arrow::array::Array as _;

pub(crate) fn first_non_null_array_index(array: &dyn arrow::array::Array) -> Option<usize> {
    (0..array.len()).find(|&index| !array.is_null(index))
}

pub(crate) fn nested_list_value(
    values: &arrow::array::ListArray,
    column: &str,
    row: usize,
    slot: usize,
) -> Result<Option<arrow::array::ArrayRef>, TracesError> {
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} row is not a list")))?;
    if slot >= row_values.len() || row_values.is_null(slot) {
        return Ok(None);
    }
    Ok(Some(row_values.value(slot)))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SpanTiming {
    pub(crate) start_ns: i64,
    pub(crate) duration_ns: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TraceTimeBounds {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

pub(crate) fn trace_time_bounds(spans: impl IntoIterator<Item = SpanTiming>) -> TraceTimeBounds {
    let mut start = None;
    let mut end = None;
    for span in spans {
        start = Some(start.map_or(span.start_ns, |current: i64| current.min(span.start_ns)));
        let span_end = span.start_ns.saturating_add(span.duration_ns);
        end = Some(end.map_or(span_end, |current: i64| current.max(span_end)));
    }
    let start = start.unwrap_or(0);
    TraceTimeBounds {
        start_ns: start,
        end_ns: end.unwrap_or(start),
    }
}

pub mod all_in_one;
pub mod blockbuilder;
pub mod compactor;
pub mod distributor;
pub mod error;
pub mod frontend;
pub mod ids;
pub mod limits;
pub mod livestore;
pub mod metrics;
pub mod metricsgen;
pub mod querier;
mod readiness;
pub mod span;
mod tempo_query_routes;
pub mod wal;
pub mod wire;

pub use blockbuilder::{
    build_blocks, group_by_trace, group_by_trace_in_record_order, object_key, prefixed_object_key,
};
pub use error::TracesError;
pub use limits::{LimitError, Limits};
pub use livestore::LiveStore;
pub use span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};
pub use wal::{SpanRecord, TRACES_WAL_TOPIC, partition_key};
pub use wire::{WireFormat, negotiate};
