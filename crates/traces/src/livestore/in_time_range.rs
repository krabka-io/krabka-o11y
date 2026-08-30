use super::{Span, UnixNano};

pub(crate) fn in_time_range(span: &Span, start_ns: UnixNano, end_ns: UnixNano) -> bool {
    start_ns.0 <= span.start_ns && span.start_ns <= end_ns.0
}
