use num_traits::ToPrimitive as _;
use promql_parser::parser::{AggregateExpr, Call, Expr};

use super::PromqlEngine;
#[cfg(feature = "experimental-functions")]
use super::annotations::{emit_warning, invalid_ratio_warning};
#[cfg(feature = "experimental-functions")]
use super::planned::PlannedInstant;
#[cfg(feature = "experimental-functions")]
use super::scalar::{DurationHelper, ScalarExtremaFn, scalar_call_to_planned};
use crate::{PromqlError, error::Result, result::QueryResult, store::MetricStore};

/// The largest `f64` Prometheus will convert to an `int64` for a `k` parameter.
///
/// This is `maxInt64` in `promql/engine.go`: `2^63 - 1024`, the last `f64`
/// below `2^63` and so the last one an `int64` can hold. Prometheus refuses a
/// parameter that is greater than or EQUAL to it, and `usize` alone does not
/// refuse it -- a `u64` reaches almost twice as far.
const MAX_INT64_PARAMETER: f64 = 9_223_372_036_854_774_784.0;

impl<S: MetricStore> PromqlEngine<S> {
    /// Plans the EXPERIMENTAL non-leaf functions onto the operator path.
    ///
    /// This method delegates to the SAME interpreter method, so the result is
    /// parity-exact by construction in value, in label set, and in any
    /// annotation side effect. It then wraps the result in the matching
    /// `Precomputed*` variant:
    ///
    /// - `max_of`/`min_of` → scalar extrema, a `PrecomputedScalar`.
    /// - `double_exponential_smoothing(m[range], sf, tf)` over a bare matrix
    ///   selector → an instant vector through the shared `apply_outer_range_fn`
    ///   fold, a `Precomputed`. `match_subquery_range_call` handles the
    ///   subquery-range form earlier.
    /// - the duration helpers `range`/`step`/`start`/`end` → a scalar. These are
    ///   plain `Expr::Call`s (NOT parser-folded) that read the scoped range
    ///   context.
    ///
    /// Returns `Ok(None)` for any other function name, and the caller then tries
    /// `plan_util_call`. A wrong-arity or invalid-argument call returns the same
    /// `Err` as the interpreter, because this method delegates to it.
    #[cfg(feature = "experimental-functions")]
    pub(super) async fn plan_experimental_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        match call.func.name {
            "max_of" => scalar_call_to_planned(
                &self
                    .eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Max)
                    .await?,
            )
            .map(Some),
            "min_of" => scalar_call_to_planned(
                &self
                    .eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Min)
                    .await?,
            )
            .map(Some),
            "double_exponential_smoothing" => Ok(Some(PlannedInstant::Precomputed(
                self.resolve_range_fold_call(tenant, call, time_ms).await?,
            ))),
            "range" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Range,
            )?)
            .map(Some),
            "step" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Step,
            )?)
            .map(Some),
            "start" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Start,
            )?)
            .map(Some),
            "end" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::End,
            )?)
            .map(Some),
            _ => Ok(None),
        }
    }

    /// Resolves the `k` of `topk`, `bottomk`, or `limitk`.
    ///
    /// Prometheus returns the empty vector for any `k` below one WITHOUT
    /// looking at it further, so `topk(0.5, v)` and `topk(-1, v)` are empty
    /// rather than errors, and it truncates the rest toward zero. Only a NaN or
    /// a value that will not fit an `int64` is refused, and the refusal text is
    /// Prometheus' own.
    pub(super) async fn eval_k_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<usize> {
        let value = self
            .eval_aggregate_scalar_parameter(tenant, aggregate, time_ms)
            .await?;
        // A NaN is not below one, so it falls through to the refusal below.
        if value < 1.0 {
            return Ok(0);
        }
        if value.is_nan() {
            return Err(PromqlError::Plan("Parameter value is NaN".to_string()));
        }
        value
            .trunc()
            .to_usize()
            .filter(|_| value < MAX_INT64_PARAMETER)
            .ok_or_else(|| PromqlError::Plan(format!("Scalar value {value} overflows int64")))
    }

    /// Resolves the ratio of `limit_ratio`.
    ///
    /// A ratio of zero selects nothing, a NaN is refused with Prometheus' own
    /// text, and a ratio outside `[-1, 1]` is capped to the nearer bound with a
    /// warning.
    #[cfg(feature = "experimental-functions")]
    pub(super) async fn eval_limit_ratio_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<f64> {
        let value = self
            .eval_aggregate_scalar_parameter(tenant, aggregate, time_ms)
            .await?;
        if value.is_nan() {
            return Err(PromqlError::Plan("Ratio value is NaN".to_string()));
        }
        let capped = value.clamp(-1.0, 1.0);
        // Matches Prometheus: warn whenever the ratio fell outside [-1, 1] and
        // had to be capped to the nearer bound.
        if !(-1.0..=1.0).contains(&value) {
            emit_warning(invalid_ratio_warning(value, capped));
        }
        Ok(capped)
    }

    /// Resolves an aggregation's scalar parameter.
    ///
    /// Prometheus evaluates the parameter as an ordinary scalar expression, so
    /// `topk(scalar(foo), v)` is as legal as `topk(3, v)`.
    pub(super) async fn eval_aggregate_scalar_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<f64> {
        let Some(param) = &aggregate.param else {
            return Err(PromqlError::Plan(format!(
                "{} requires a numeric parameter",
                aggregate.op
            )));
        };
        self.eval_scalar_expr(
            tenant,
            param,
            time_ms,
            &format!("{} parameter", aggregate.op),
        )
        .await
    }

    #[cfg(feature = "experimental-functions")]
    pub(super) fn eval_duration_helper_call(
        call: &Call,
        time_ms: i64,
        helper: DurationHelper,
    ) -> Result<QueryResult> {
        if !call.args.args.is_empty() {
            return Err(PromqlError::Plan(format!(
                "{} expects no arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: helper.value_ms().to_f64().unwrap_or(f64::MAX) / 1000.0,
        })
    }

    #[cfg(feature = "experimental-functions")]
    pub(super) async fn eval_scalar_extrema_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: ScalarExtremaFn,
    ) -> Result<QueryResult> {
        let [left_arg, right_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly two arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let left = self
            .eval_scalar_expr(tenant, left_arg, time_ms, call.func.name)
            .await?;
        let right = self
            .eval_scalar_expr(tenant, right_arg, time_ms, call.func.name)
            .await?;
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: kind.apply(left, right),
        })
    }

    pub(super) async fn eval_scalar_arg(
        &self,
        tenant: &str,
        call: &Call,
        index: usize,
        time_ms: i64,
        name: &str,
    ) -> Result<f64> {
        match self
            .plan_and_resolve(tenant, &call.args.args[index], time_ms)
            .await?
        {
            QueryResult::Scalar { value, .. } => Ok(value),
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => Err(PromqlError::Plan(format!(
                "{} {name} argument must be a scalar",
                call.func.name
            ))),
        }
    }

    pub(super) async fn eval_scalar_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
        name: &str,
    ) -> Result<f64> {
        match self.plan_and_resolve(tenant, expr, time_ms).await? {
            QueryResult::Scalar { value, .. } => Ok(value),
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => Err(PromqlError::Plan(format!(
                "{name} argument must be a scalar"
            ))),
        }
    }
}
