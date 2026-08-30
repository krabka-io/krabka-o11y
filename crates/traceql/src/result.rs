//! Tempo-shaped `TraceQL` result model.

use krabka_units::{ByteSize, Time};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{bytes, millis, nanos};

    use super::*;

    #[test]
    fn span_ref_holds_typed_attributes() {
        let s = SpanRef {
            span_id: [1; 8],
            parent_span_id: None,
            name: "op".into(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 1000,
            duration: nanos(42),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: vec![
                ("http.status_code".into(), AttrValue::Int(200)),
                ("ok".into(), AttrValue::Bool(true)),
            ],
            events: Vec::new(),
            links: Vec::new(),
        };
        assert!(s.attributes[0].1 == AttrValue::Int(200));
        assert!(s.attributes[1].1 == AttrValue::Bool(true));
    }

    #[test]
    fn search_response_nests_span_sets() {
        let resp = SearchResponse {
            traces: vec![TraceResult {
                trace_id: [0xAB; 16],
                root_service_name: "checkout".into(),
                root_trace_name: "POST /pay".into(),
                start_time_unix_nano: 5,
                duration: millis(12),
                span_sets: vec![SpanSet {
                    spans: vec![],
                    matched: 3,
                }],
            }],
            inspected_traces: 1,
            inspected: bytes(4096),
        };
        assert!(resp.traces[0].span_sets[0].matched == 3);
        assert!(resp.traces[0].trace_id == [0xAB; 16]);
        assert!(resp.inspected_traces == 1);
        assert!(resp.inspected == bytes(4096));
    }

    #[test]
    fn tag_scope_is_copy() {
        let s = TagScope::Span;
        let c = s;
        assert!(s == TagScope::Span);
        assert!(c == TagScope::Span);
    }
}

// === split-modules: generated submodules ===
mod attr_value;
mod event_ref;
mod link_ref;
mod scoped_tag;
mod search_response;
mod span_ref;
mod span_set;
mod tag_scope;
mod trace_metric_exemplar;
mod trace_metric_series;
mod trace_metrics_response;
mod trace_result;
mod trace_spans;
mod typed_value;

pub use attr_value::AttrValue;
pub use event_ref::EventRef;
pub use link_ref::LinkRef;
pub use scoped_tag::ScopedTag;
pub use search_response::SearchResponse;
pub use span_ref::SpanRef;
pub use span_set::SpanSet;
pub use tag_scope::TagScope;
pub use trace_metric_exemplar::TraceMetricExemplar;
pub use trace_metric_series::TraceMetricSeries;
pub use trace_metrics_response::TraceMetricsResponse;
pub use trace_result::TraceResult;
pub use trace_spans::TraceSpans;
pub use typed_value::TypedValue;
