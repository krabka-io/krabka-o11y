// A server span of the `api` service, for the block-building suites.

use krabka_traces::{Span, SpanRecord};

use crate::api_span::ApiSpan;

/// A server span of the `api` service in `trace_id`, with id and name taken
/// from `span_id` and an optional parent of the same trace.
pub struct FixtureSpan {
    pub trace_id: [u8; 16],
    pub span_id: u8,
    pub parent: Option<u8>,
    pub start_ns: i64,
}

impl FixtureSpan {
    pub fn build(self) -> Span {
        let Self {
            trace_id,
            span_id,
            parent,
            start_ns,
        } = self;
        Span {
            parent_span_id: parent.map(|id| [id; 8]),
            ..ApiSpan {
                trace_id,
                span_id,
                start_ns,
                duration_ns: 5,
            }
            .build()
        }
    }

    /// This span as a WAL record of `tenant`.
    pub fn record(self, tenant: &str) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            span: self.build(),
        }
    }
}
