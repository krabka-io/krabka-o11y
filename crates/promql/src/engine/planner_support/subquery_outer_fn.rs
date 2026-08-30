use super::{OuterRangeFn, Expr};

/// The outer range or `*_over_time` function of a subquery call.
///
/// The call has the form `f(inner[range:res] ...)`, and every scalar parameter
/// is still unresolved. The async planner method resolves the parameter argument
/// `Expr`s through the interpreter, in the same way as the corresponding
/// `eval_*_call`.
pub(crate) enum SubqueryOuterFn<'a> {
    /// A function whose only argument is the range vector. The name fully
    /// determines the [`OuterRangeFn`].
    NoParam(OuterRangeFn),
    /// `quantile_over_time(phi, inner[...])`. Resolve `phi`, the leading argument.
    QuantileOverTime { phi: &'a Expr },
    /// `predict_linear(inner[...], t)`. Resolve the trailing duration argument.
    PredictLinear { duration: &'a Expr },
    /// `double_exponential_smoothing(inner[...], sf, tf)`. Resolve both factors.
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: &'a Expr,
        trend: &'a Expr,
    },
}
