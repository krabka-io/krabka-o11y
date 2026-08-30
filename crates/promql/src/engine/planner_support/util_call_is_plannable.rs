use super::*;

/// Structural gate for the float UTILITY functions that
/// `PromqlEngine::plan_util_call` handles.
///
/// The functions are `time`/`pi` (argless), `scalar`/`vector`, `timestamp`, the
/// calendar family (argless or one vector arg), and `absent`/`absent_over_time`.
/// The inner instant-vector argument, where one exists, must itself be
/// structurally plannable. A data-dependent shape, such as a histogram series,
/// falls back per step inside the planner. A `vector` argument must be
/// scalar-typed. Any other function, or a non-matching arity, returns `false`,
/// and the dispatch falls through to the interpreter.
pub(crate) fn util_call_is_plannable(call: &Call) -> bool {
    match call.func.name {
        // Argless scalar utilities.
        "time" | "pi" => call.args.args.is_empty(),
        // The lone inner instant-vector argument must be plannable.
        "scalar" | "timestamp" | "absent" => call
            .args
            .args
            .first()
            .is_some_and(|arg| call.args.args.len() == 1 && instant_expr_is_plannable(arg)),
        // `vector(s)` takes a scalar argument resolved through the interpreter.
        "vector" => {
            call.args.args.len() == 1 && call.args.args[0].value_type() == ValueType::Scalar
        }
        // `absent_over_time(v[range])`: a plain float-only matrix selector rides
        // the fast `eval_range_arg` path; a histogram-bearing matrix, a subquery
        // range, or an anchored/smoothed selector delegates to the interpreter's
        // `eval_absent_over_time_call` (parity-exact). All range-vector shapes are
        // plannable; the per-shape / wrong-arity error is raised inside
        // `plan_absent_over_time_call`.
        "absent_over_time" => {
            let [arg] = call.args.args.as_slice() else {
                return false;
            };
            let mut inner = arg.as_ref();
            while let Expr::Paren(paren) = inner {
                inner = paren.expr.as_ref();
            }
            matches!(
                inner,
                Expr::MatrixSelector(_) | Expr::Subquery(_) | Expr::Extension(_)
            )
        }
        // The calendar family: argless (operates on `time()`) or one plannable
        // inner vector argument.
        other if calendar_fn_from_function_name(other).is_some() => match call.args.args.as_slice()
        {
            [] => true,
            [arg] => instant_expr_is_plannable(arg),
            _ => false,
        },
        _ => false,
    }
}
