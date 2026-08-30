use super::{ByteSize, f64_from_usize, kibibytes};

/// The assembled-trace ceiling. It budgets about 64KiB of OTLP JSON per span.
pub(crate) fn max_trace_size(spans: usize) -> ByteSize {
    kibibytes(64) * f64_from_usize(spans)
}
