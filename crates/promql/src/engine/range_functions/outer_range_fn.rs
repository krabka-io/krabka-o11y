use super::{IrateFn, OverTimeFn, RangeFn, Time};

/// A range or `*_over_time` function applied to an evaluated range vector.
///
/// The range vector is a [`RangeEval`]. This type holds any scalar parameters
/// the caller resolved. It is the outer half of a range-function evaluation:
/// the per-series fold that turns each window of `(end - range, end]` samples
/// into one instant sample. The interpreter (`eval_*_call`) and the recursive
/// planner's subquery dispatch both build one of these and apply it through
/// [`apply_outer_range_fn`]. The operator path therefore matches the
/// interpreter byte-for-byte for any range vector it gets.
///
/// `absent` and `absent_over_time` build an absent-labels series, and the
/// scalar-typed helpers `time` and `pi` return scalars. None of them are
/// range-vector folds, so this type does not cover them. The experimental
/// `double_exponential_smoothing` holds its two factors. The non-experimental
/// build cannot reach it.
#[derive(Clone, Copy)]
pub(crate) enum OuterRangeFn {
    Range(RangeFn),
    InstantDelta(IrateFn),
    Deriv,
    OverTime(OverTimeFn),
    QuantileOverTime(f64),
    PredictLinear(Time),
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: f64,
        trend: f64,
    },
}
