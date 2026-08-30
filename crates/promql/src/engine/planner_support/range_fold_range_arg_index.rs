use super::Call;

/// Returns the index of the range-vector argument of a range fold, by name.
///
/// The fold is a range or `*_over_time` fold with one range-vector argument.
/// This is the residual set of range-fold functions whose operator routing is
/// NOT a fast UDF chain. Either the function has no operator-leaf lowering
/// (`changes`/`resets`/`deriv`/`predict_linear`/`double_exponential_smoothing`),
/// or its argument is an `anchored`/`smoothed` extended selector.
/// `match_rate_range_call` and `match_over_time_range_call` reject such a
/// selector, because they need a plain `MatrixSelector`. The delegated
/// interpreter method resolves the parameter arguments, if the call has any.
pub(crate) fn range_fold_range_arg_index(call: &Call) -> Option<usize> {
    match call.func.name {
        // One argument: the range vector.
        "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" | "deriv"
        | "sum_over_time" | "avg_over_time" | "count_over_time" | "min_over_time"
        | "max_over_time" | "stddev_over_time" | "stdvar_over_time" | "last_over_time"
        | "present_over_time" => (call.args.args.len() == 1).then_some(0),
        #[cfg(feature = "experimental-functions")]
        "mad_over_time"
        | "first_over_time"
        | "ts_of_first_over_time"
        | "ts_of_last_over_time"
        | "ts_of_min_over_time"
        | "ts_of_max_over_time" => (call.args.args.len() == 1).then_some(0),
        // `quantile_over_time(phi, range)`: the range vector is the SECOND arg.
        "quantile_over_time" => (call.args.args.len() == 2).then_some(1),
        // `predict_linear(range, t)`: the range vector is the FIRST arg.
        "predict_linear" => (call.args.args.len() == 2).then_some(0),
        // `double_exponential_smoothing(range, sf, tf)`: range is the FIRST arg.
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => (call.args.args.len() == 3).then_some(0),
        _ => None,
    }
}
