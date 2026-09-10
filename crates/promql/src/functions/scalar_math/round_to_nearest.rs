/// Rounds `value` to the nearest multiple of `to_nearest`, ties going up.
///
/// Prometheus INVERTS the step and multiplies, rather than dividing by it:
/// `math.Floor(f*toNearestInverse+0.5) / toNearestInverse`. The two spellings
/// disagree in the last bits for a step such as `0.1`, whose inverse is exact
/// while the step itself is not, so the inversion is part of the answer.
pub(crate) fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    let inverse = 1.0 / to_nearest;
    (value * inverse + 0.5).floor() / inverse
}
