use super::*;

/// The absolute split window a timestamp belongs to.
///
/// The window is the greatest multiple of `split_interval` that is `<= ts`. This
/// function uses flooring division, so negative timestamps still align downward.
pub(crate) fn absolute_split_window(ts: i64, split_interval: i64) -> i64 {
    let quotient = ts.div_euclid(split_interval);
    quotient.saturating_mul(split_interval)
}
