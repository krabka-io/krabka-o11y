use super::*;

pub(crate) fn f64_from_usize(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
