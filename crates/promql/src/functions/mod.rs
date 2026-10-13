//! `PromQL` functions implemented as `DataFusion` `ScalarUDF`s over the
//! windowed columns that [`crate::extension::range_manipulate`] emits.
//!
//! The rate family (`rate`, `increase`, `delta`, `irate`, `idelta`) is in
//! [`rate`]. The shared extrapolation math, which matches the interpreter, is in
//! [`extrapolate`]. The `*_over_time` family (`sum`, `avg`, `count`, `min`,
//! `max`, `stddev`, `stdvar`, `last`, `present`, and `quantile_over_time`) is in
//! [`over_time`], a port of the engine's per-window reductions.
//! [`crate::planner::rate_range`] lowers a top-level `f(selector[range])`
//! instant query over float-only series onto these UDFs. The planner registers
//! it on [`crate::extension::planner::prom_session_context`]. Every nested or
//! histogram-bearing form stays on the tree-walking interpreter.

pub mod aggregate_udaf;
pub mod extrapolate;
pub mod over_time;
pub mod rate;
pub mod scalar_math;
mod udf_args;
#[cfg(test)]
mod udf_test_support;

use arrow::datatypes::DataType;

#[derive(Clone, Copy)]
pub(crate) enum UdfArityPhase {
    Plan,
    Execution,
}

pub(crate) fn check_udf_arity(
    name: &str,
    expected: usize,
    actual: usize,
    phase: UdfArityPhase,
) -> datafusion::common::Result<()> {
    if actual == expected {
        return Ok(());
    }
    let message = format!("{name} expects {expected} arguments, got {actual}");
    Err(match phase {
        UdfArityPhase::Plan => datafusion::common::DataFusionError::Plan(message),
        UdfArityPhase::Execution => datafusion::common::DataFusionError::Execution(message),
    })
}

pub(crate) fn coerce_udf_arg_types(
    name: &str,
    expected: usize,
    arg_types: &[DataType],
) -> datafusion::common::Result<Vec<DataType>> {
    check_udf_arity(name, expected, arg_types.len(), UdfArityPhase::Plan)?;
    Ok(arg_types.to_vec())
}

pub(crate) fn check_udf_execution_args(
    name: &str,
    expected: usize,
    actual: usize,
) -> datafusion::common::Result<()> {
    check_udf_arity(name, expected, actual, UdfArityPhase::Execution)
}

macro_rules! impl_window_udf_identity {
    () => {
        fn name(&self) -> &str {
            self.family.udf_name()
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
            Ok(DataType::Float64)
        }
    };
}

pub use aggregate_udaf::{prom_max_udaf, prom_min_udaf, register_aggregate_udafs};
pub(crate) use impl_window_udf_identity;
pub use over_time::{OverTimeFamily, register_over_time_udfs};
pub use rate::{
    delta_udf, idelta_udf, increase_udf, irate_udf, rate_family_udfs, rate_udf, register_rate_udfs,
};
pub use scalar_math::{ScalarMathOp, register_scalar_math_udfs};
