use super::{TraceIdHigh, TraceIdLow};

pub(crate) fn trace_id(high: TraceIdHigh, low: TraceIdLow) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&high.0.to_be_bytes());
    out[8..].copy_from_slice(&low.0.to_be_bytes());
    out
}
