use super::*;

/// Port of the interpreter's `round_to_nearest`.
pub(crate) fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    (value / to_nearest + 0.5).floor() * to_nearest
}
