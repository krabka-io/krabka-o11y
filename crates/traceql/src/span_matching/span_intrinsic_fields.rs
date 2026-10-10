use super::{InputSpan, SpanRef, Time};

/// The span intrinsics that a tag-values lookup reads off one span alone,
/// without its position in the trace's nested set.
#[derive(Clone, Copy, Debug)]
pub struct SpanIntrinsicFields<'a> {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'a str,
    pub kind: i32,
    pub duration: Time,
    pub status_code: i32,
    pub status_message: &'a str,
    pub instrumentation_name: &'a str,
    pub instrumentation_version: &'a str,
}

impl<'a> From<&'a InputSpan> for SpanIntrinsicFields<'a> {
    fn from(span: &'a InputSpan) -> Self {
        Self {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: &span.name,
            kind: span.kind,
            duration: span.duration,
            status_code: span.status_code,
            status_message: &span.status_message,
            instrumentation_name: &span.instrumentation_name,
            instrumentation_version: &span.instrumentation_version,
        }
    }
}

impl<'a> From<&'a SpanRef> for SpanIntrinsicFields<'a> {
    fn from(span: &'a SpanRef) -> Self {
        Self {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: &span.name,
            kind: span.kind,
            duration: span.duration,
            status_code: span.status_code,
            status_message: &span.status_message,
            instrumentation_name: &span.instrumentation_name,
            instrumentation_version: &span.instrumentation_version,
        }
    }
}
