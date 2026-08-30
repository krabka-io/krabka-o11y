
/// Returns true if `phi` is a valid quantile in `[0, 1]`.
///
/// The engine still evaluates an out-of-range or NaN phi. Prometheus returns
/// `+/-Inf` or `NaN` with an `InvalidQuantileWarning` and does not error. This
/// function only gates the warning.
pub(crate) fn is_valid_quantile(phi: f64) -> bool {
    (0.0..=1.0).contains(&phi)
}
