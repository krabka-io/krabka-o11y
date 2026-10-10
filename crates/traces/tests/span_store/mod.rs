// An in-memory span store built from WAL span records, the way the querier
// reads them, for suites that query what a distributor accepted.

use krabka_traceql::{
    AttrValue as TraceqlAttrValue, EventRef, InMemorySpanStore, InputSpan, LinkRef,
};
use krabka_traces::{AttrValue, Span, SpanRecord, group_by_trace_in_record_order};
use krabka_units::{Time, convert::TimeExt as _};

pub fn span_store_from_records(records: &[SpanRecord]) -> InMemorySpanStore {
    let mut store = InMemorySpanStore::new();
    for ((tenant, _), spans) in group_by_trace_in_record_order(records) {
        let root = spans
            .iter()
            .find(|span| span.parent_span_id.is_none())
            .unwrap_or(&spans[0]);
        let root_service = resource_attr(root, "service.name")
            .unwrap_or("unknown")
            .to_string();
        let root_name = root.name.clone();
        store.push_trace(
            &tenant,
            &root_service,
            &root_name,
            spans.into_iter().map(input_span).collect(),
        );
    }
    store
}

fn input_span(span: Span) -> InputSpan {
    let mut attrs = span.resource_attrs;
    attrs.extend(span.span_attrs);
    InputSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        kind: span.kind.as_i32(),
        start_unix_nano: span.start_ns,
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message,
        instrumentation_name: span.instrumentation_scope,
        instrumentation_version: span.instrumentation_version,
        attrs: attrs
            .into_iter()
            .filter_map(|attr| Some((attr.key, traceql_attr(&attr.value)?)))
            .collect(),
        events: span
            .events
            .into_iter()
            .map(|event| EventRef {
                name: event.name,
                time_since_start: Time::from_nanos(event.time_unix_nano - span.start_ns),
                attributes: event
                    .attrs
                    .into_iter()
                    .filter_map(|attr| Some((attr.key, traceql_attr(&attr.value)?)))
                    .collect(),
            })
            .collect(),
        links: span
            .links
            .into_iter()
            .map(|link| LinkRef {
                trace_id: link.trace_id,
                span_id: link.span_id,
                attributes: link
                    .attrs
                    .into_iter()
                    .filter_map(|attr| Some((attr.key, traceql_attr(&attr.value)?)))
                    .collect(),
            })
            .collect(),
    }
}

pub fn traceql_attr(value: &AttrValue) -> Option<TraceqlAttrValue> {
    value.traceql_value()
}

pub fn resource_attr<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.resource_attrs
        .iter()
        .find_map(|attr| match &attr.value {
            AttrValue::Str(value) if attr.key == key => Some(value.as_str()),
            _ => None,
        })
}
