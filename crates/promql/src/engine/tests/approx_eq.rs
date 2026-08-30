pub(crate) fn approx_eq(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return left == right;
    }
    // Relative tolerance: an absolute `f64::EPSILON` bound is too tight for
    // magnitudes above ~1 — a Kahan/Welford-compensated fold (matching
    // Prometheus) rounds in the last ULP, e.g. a population variance of 4.0
    // lands at 3.999999999999_9996. Scale the bound by operand magnitude.
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= f64::EPSILON * 4.0 * scale
}
