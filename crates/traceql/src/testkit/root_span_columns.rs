use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray};

use super::Arc;

/// One span of a [`root_span_columns`] fixture.
///
/// The fields are the span-schema columns that tell spans apart. Every
/// other column holds the same value for each span.
#[derive(Clone, Copy, Debug, Default)]
pub struct RootSpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub root_service_name: &'static str,
    pub root_span_name: &'static str,
    pub trace_start_ns: i64,
    pub trace_duration_ns: i64,
    pub name: &'static str,
    pub kind: i32,
    pub start_ns: i64,
    pub duration_ns: i64,
}

/// Builds the span-schema columns from `trace_id` through
/// `instrumentation_version` for `rows`, in schema order.
///
/// Each span is a root with no children: its parent span id is null, its
/// nested set is `[1, 2]` with parent index `0`, its status is unset, and its
/// instrumentation scope is `tracer` with no version. The caller appends any
/// attribute columns and supplies the schema.
///
/// # Panics
///
/// Panics if Arrow rejects a fixed-size id, which the id types rule out.
#[must_use]
pub fn root_span_columns(rows: &[RootSpanRow]) -> Vec<ArrayRef> {
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 8);
    let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 8);
    for row in rows {
        trace_id
            .append_value(row.trace_id)
            .expect("16-byte trace id");
        span_id.append_value(row.span_id).expect("8-byte span id");
        parent_span_id.append_null();
    }
    let int32 = |value: i32| Arc::new(Int32Array::from(vec![value; rows.len()])) as ArrayRef;
    let text = |value: &str| Arc::new(StringArray::from(vec![value; rows.len()])) as ArrayRef;
    let int64_of = |value: fn(&RootSpanRow) -> i64| {
        Arc::new(rows.iter().map(value).collect::<Int64Array>()) as ArrayRef
    };
    let text_of = |value: fn(&RootSpanRow) -> &'static str| {
        Arc::new(
            rows.iter()
                .map(|row| Some(value(row)))
                .collect::<StringArray>(),
        ) as ArrayRef
    };
    vec![
        Arc::new(trace_id.finish()) as ArrayRef,
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        int32(1),
        int32(2),
        int32(0),
        int32(0),
        text_of(|row| row.root_service_name),
        text_of(|row| row.root_span_name),
        int64_of(|row| row.trace_start_ns),
        int64_of(|row| row.trace_duration_ns),
        text_of(|row| row.name),
        Arc::new(rows.iter().map(|row| row.kind).collect::<Int32Array>()),
        int64_of(|row| row.start_ns),
        int64_of(|row| row.duration_ns),
        int32(0),
        text(""),
        text("tracer"),
        text(""),
    ]
}
