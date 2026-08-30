use super::{ByteSize, kibibytes, f64_from_usize};

/// The assembled-trace ceiling. It budgets about 64KiB of OTLP JSON per span.
pub(crate) fn max_trace_size(spans: usize) -> ByteSize {
    kibibytes(64) * f64_from_usize(spans)
}
