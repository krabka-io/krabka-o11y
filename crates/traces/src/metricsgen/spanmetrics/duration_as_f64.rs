use super::*;

pub(crate) fn duration_as_f64(duration_ns: i64) -> f64 {
    duration_ns.max(0).to_f64().unwrap_or(f64::MAX)
}
