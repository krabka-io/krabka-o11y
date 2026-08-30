//! Storage boundary for `TraceQL` planning and execution.

use datafusion::prelude::SessionContext;
use krabka_units::ByteSize;

use crate::{
    error::Result,
    result::{ScopedTag, TagScope, TraceSpans, TypedValue},
};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use datafusion::prelude::SessionContext;
    use krabka_units::bytes;

    use super::*;

    struct Empty;

    #[async_trait::async_trait]
    impl SpanStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[SpanMatcher],
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<ScanResult> {
            Ok(ScanResult {
                ctx: SessionContext::new(),
                span_table: "spans".into(),
                inspected: bytes(0),
            })
        }

        async fn trace_by_id(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<crate::result::TraceSpans>> {
            Ok(None)
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<crate::result::TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::ScopedTag>> {
            Ok(vec![])
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::TypedValue>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let s: std::sync::Arc<dyn SpanStore> = std::sync::Arc::new(Empty);
        let r = s.scan("t", &[], 0, 1).await.unwrap();
        assert!(r.span_table == "spans");
        assert!(s.trace_by_id("t", &[0; 16]).await.unwrap().is_none());
    }
}

mod filter_trace_spans_by_time;
mod match_cmp;
mod match_scope;
mod match_value;
mod scan_job;
mod scan_options;
mod scan_result;
mod span_matcher;
mod span_store;

pub use filter_trace_spans_by_time::filter_trace_spans_by_time;
pub use match_cmp::MatchCmp;
pub use match_scope::MatchScope;
pub use match_value::MatchValue;
pub use scan_job::ScanJob;
pub use scan_options::ScanOptions;
pub use scan_result::ScanResult;
pub use span_matcher::SpanMatcher;
pub use span_store::SpanStore;
