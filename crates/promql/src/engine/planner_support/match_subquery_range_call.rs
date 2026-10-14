use super::{Call, Expr, SubqueryExpr, SubqueryOuterFn, no_param_outer_range_fn};

pub(crate) struct SubqueryRangeCall<'a> {
    pub subquery: &'a SubqueryExpr,
    pub outer: SubqueryOuterFn<'a>,
}

struct SubqueryRangeArguments<'a> {
    range: &'a Expr,
    outer: SubqueryOuterFn<'a>,
}

/// Recognizes a `f(inner[range:resolution] ...)` call over a subquery.
///
/// The range argument is a subquery, and the outer `f` is a planner-supported
/// range or `*_over_time` fold. This function returns the [`SubqueryExpr`] and
/// the outer-fn spec, whose parameters are unresolved. A call is eligible if and
/// only if `expr` is a [`Call`] whose function `f` is one of the supported folds
/// and whose range argument is an [`Expr::Subquery`] after the removal of
/// parentheses. `absent_over_time`, which synthesizes absent labels, and every
/// non-fold function return `None` and stay on the interpreter. A
/// matrix-selector range argument also returns `None`, and
/// [`match_rate_range_call`] and [`match_over_time_range_call`] match it instead.
pub(crate) fn match_subquery_range_call(call: &Call) -> Option<SubqueryRangeCall<'_>> {
    // Resolve the range-vector argument's position and the parameter args by the
    // function's arity, exactly as the corresponding `eval_*_call` does.
    let arguments = match call.func.name {
        "quantile_over_time" => {
            let [phi, range] = call.args.args.as_slice() else {
                return None;
            };
            SubqueryRangeArguments {
                range: range.as_ref(),
                outer: SubqueryOuterFn::QuantileOverTime { phi },
            }
        }
        "predict_linear" => {
            let [range, duration] = call.args.args.as_slice() else {
                return None;
            };
            SubqueryRangeArguments {
                range: range.as_ref(),
                outer: SubqueryOuterFn::PredictLinear { duration },
            }
        }
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => {
            let [range, smoothing, trend] = call.args.args.as_slice() else {
                return None;
            };
            SubqueryRangeArguments {
                range: range.as_ref(),
                outer: SubqueryOuterFn::DoubleExponentialSmoothing { smoothing, trend },
            }
        }
        name => {
            let outer = no_param_outer_range_fn(name)?;
            let [range] = call.args.args.as_slice() else {
                return None;
            };
            SubqueryRangeArguments {
                range: range.as_ref(),
                outer: SubqueryOuterFn::NoParam(outer),
            }
        }
    };
    let mut arg = arguments.range;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::Subquery(subquery) = arg else {
        return None;
    };
    Some(SubqueryRangeCall {
        subquery,
        outer: arguments.outer,
    })
}
