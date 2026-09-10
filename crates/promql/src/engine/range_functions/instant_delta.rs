use super::IrateFn;

/// Folds the last two float samples of a window into `irate` or `idelta`.
///
/// `irate` reads a fall in value as a counter reset and takes the newer sample
/// whole, then divides by the interval between the two. `idelta` is meant for
/// gauges, so it reports the signed difference as it stands.
pub(crate) fn instant_delta(previous: f64, last: f64, interval_secs: f64, kind: IrateFn) -> f64 {
    let mut result = last - previous;
    if matches!(kind, IrateFn::Irate) {
        if result < 0.0 {
            result = last;
        }
        result /= interval_secs;
    }
    result
}
