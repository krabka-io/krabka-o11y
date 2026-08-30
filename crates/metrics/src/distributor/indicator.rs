use super::*;

/// The Prometheus encoding of a boolean: `1` when it holds, `0` when it does
/// not.
pub(crate) fn indicator(holds: bool) -> f64 {
    f64::from(u8::from(holds))
}
