use super::*;

pub(crate) fn is_zero(value: f64) -> bool {
    value.abs() <= f64::EPSILON
}
