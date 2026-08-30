//! `PromQL` `min`/`max` aggregations as `DataFusion` [`AggregateUDF`]s.
//!
//! Arrow and `DataFusion`'s built-in `min`/`max` order floats with `total_cmp`,
//! which places NaN at the extremes and therefore propagates NaN into the
//! result. A NaN sample can become the reported `max`, and a group's `min`/`max`
//! is NaN when any sample is NaN. Prometheus does the opposite: `min`/`max`
//! ignore NaN. A group's extremum is over its non-NaN samples, and the result is
//! NaN only when every sample in the group is NaN.
//!
//! The `prom_min` and `prom_max` UDAFs reproduce Prometheus' aggregation loop in
//! `promql/engine.go` exactly. The operator path therefore agrees bit-for-bit
//! with the tree-walking interpreter's [`crate::engine`] `AggregateState`:
//!
//! - The first observed sample seeds the running extremum, NaN included.
//! - Each later sample `f` replaces the running value `r` when `r {>,<} f`, the
//!   float comparison the new sample wins, or when `r` is NaN. `NaN > _` and
//!   `NaN < _` are both false, so a non-NaN sample always displaces a NaN seed,
//!   and a NaN sample never displaces an existing non-NaN extremum.
//! - An empty group makes no accumulator output here, because the planner's
//!   grouping guarantees that every emitted group has at least one row. An
//!   all-NaN group keeps NaN.
//!
//! Signed zero matches Prometheus. `0.0 {>,<} -0.0` is false, so the first
//! observed zero is kept, and neither sign displaces the other.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::{DataType, Float64Type},
};
use datafusion::{
    common::{Result as DfResult, ScalarValue},
    logical_expr::{Accumulator, AggregateUDF, Volatility, create_udaf, function::AccumulatorArgs},
    prelude::SessionContext,
};

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Float64Array};
    use datafusion::execution::FunctionRegistry;

    use super::*;

    fn run(extremum: Extremum, samples: &[f64]) -> ScalarValue {
        let mut acc = PromExtremumAccumulator::new(extremum);
        let array: ArrayRef = Arc::new(Float64Array::from(samples.to_vec()));
        acc.update_batch(&[array]).unwrap();
        acc.evaluate().unwrap()
    }

    fn float(value: ScalarValue) -> f64 {
        match value {
            ScalarValue::Float64(Some(value)) => value,
            other => panic!("expected non-null Float64, got {other:?}"),
        }
    }

    /// Compares two floats bit-exactly, so the result must equal `expected`.
    ///
    /// This avoids clippy's `float_cmp` lint and stays precise for the
    /// integer-valued and signed-zero cases under test.
    fn bits_eq(value: f64, expected: f64) -> bool {
        value.to_bits() == expected.to_bits()
    }

    #[test]
    fn ignores_nan_in_mixed_group() {
        // min/max are taken over the non-NaN values; NaN appearing first then
        // non-NaN still yields the non-NaN extremum.
        for (extremum, samples, want) in [
            (Extremum::Min, &[f64::NAN, 3.0, 1.0, f64::NAN][..], 1.0),
            (Extremum::Max, &[f64::NAN, 3.0, 1.0, f64::NAN][..], 3.0),
            (Extremum::Min, &[f64::NAN, 5.0][..], 5.0),
            (Extremum::Max, &[f64::NAN, 5.0][..], 5.0),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, samples)), want));
        }
    }

    #[test]
    fn all_nan_group_yields_nan() {
        // A single NaN sample is still NaN (seen, never displaced).
        for (extremum, samples) in [
            (Extremum::Min, &[f64::NAN, f64::NAN][..]),
            (Extremum::Max, &[f64::NAN, f64::NAN][..]),
            (Extremum::Min, &[f64::NAN][..]),
            (Extremum::Max, &[f64::NAN][..]),
        ] {
            assert2::assert!(float(run(extremum, samples)).is_nan());
        }
    }

    #[test]
    fn handles_infinities() {
        assert2::assert!(
            float(run(Extremum::Min, &[f64::INFINITY, 1.0, f64::NEG_INFINITY])).is_infinite()
        );
        for (extremum, samples, want) in [
            (
                Extremum::Min,
                &[1.0, f64::NEG_INFINITY][..],
                f64::NEG_INFINITY,
            ),
            (Extremum::Max, &[1.0, f64::INFINITY][..], f64::INFINITY),
            (
                Extremum::Max,
                &[f64::NEG_INFINITY, f64::NAN][..],
                f64::NEG_INFINITY,
            ),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, samples)), want));
        }
    }

    #[test]
    fn signed_zero_keeps_first_seen() {
        // `0.0 {>,<} -0.0` is false, so the first-observed zero is retained,
        // matching Prometheus and the interpreter.
        for (extremum, samples, want) in [
            (Extremum::Min, [0.0, -0.0], 0.0),
            (Extremum::Min, [-0.0, 0.0], -0.0),
            (Extremum::Max, [0.0, -0.0], 0.0),
            (Extremum::Max, [-0.0, 0.0], -0.0),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, &samples)), want));
        }
    }

    #[test]
    fn empty_group_evaluates_null() {
        let mut acc = PromExtremumAccumulator::new(Extremum::Min);
        let empty: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        acc.update_batch(&[empty]).unwrap();
        assert2::assert!(acc.evaluate().unwrap() == ScalarValue::Float64(None));
    }

    #[test]
    fn accumulator_size_reports_struct_size() {
        let acc = PromExtremumAccumulator::new(Extremum::Min);
        assert2::assert!(acc.size() == std::mem::size_of_val(&acc));
        assert2::assert!(acc.size() > 1);
    }

    #[test]
    fn merge_of_partial_states_matches_single_pass() {
        // Two partitions: one all-NaN, one with finite values. The merged result
        // is the min over the finite values (the all-NaN partition contributes a
        // NaN running value that is displaced).
        let mut left = PromExtremumAccumulator::new(Extremum::Min);
        left.update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN])) as ArrayRef])
            .unwrap();
        let mut right = PromExtremumAccumulator::new(Extremum::Min);
        right
            .update_batch(&[Arc::new(Float64Array::from(vec![4.0, 2.0])) as ArrayRef])
            .unwrap();

        let left_state = left.state().unwrap();
        let right_state = right.state().unwrap();
        let running = Arc::new(Float64Array::from(vec![
            match left_state[0] {
                ScalarValue::Float64(value) => value,
                _ => unreachable!(),
            },
            match right_state[0] {
                ScalarValue::Float64(value) => value,
                _ => unreachable!(),
            },
        ])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![
            match left_state[1] {
                ScalarValue::Boolean(value) => value,
                _ => unreachable!(),
            },
            match right_state[1] {
                ScalarValue::Boolean(value) => value,
                _ => unreachable!(),
            },
        ])) as ArrayRef;

        let mut merged = PromExtremumAccumulator::new(Extremum::Min);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(bits_eq(float(merged.evaluate().unwrap()), 2.0));
    }

    #[test]
    fn merge_ignores_unseen_partition_even_with_running_value() {
        let running = Arc::new(Float64Array::from(vec![7.0])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![false])) as ArrayRef;

        let mut merged = PromExtremumAccumulator::new(Extremum::Min);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(merged.evaluate().unwrap() == ScalarValue::Float64(None));
    }

    #[test]
    fn merge_of_all_nan_partitions_stays_nan() {
        let mut a = PromExtremumAccumulator::new(Extremum::Max);
        a.update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef])
            .unwrap();
        let a_state = a.state().unwrap();
        let running = Arc::new(Float64Array::from(vec![match a_state[0] {
            ScalarValue::Float64(value) => value,
            _ => unreachable!(),
        }])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![match a_state[1] {
            ScalarValue::Boolean(value) => value,
            _ => unreachable!(),
        }])) as ArrayRef;
        let mut merged = PromExtremumAccumulator::new(Extremum::Max);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(float(merged.evaluate().unwrap()).is_nan());
    }

    #[test]
    fn register_installs_min_and_max_udafs() {
        let ctx = SessionContext::new();
        register_aggregate_udafs(&ctx);

        assert2::assert!(ctx.udaf(PROM_MIN_UDAF_NAME).is_ok());
        assert2::assert!(ctx.udaf(PROM_MAX_UDAF_NAME).is_ok());
    }
}

mod extremum;
mod extremum_udaf;
mod prom_extremum_accumulator;
mod prom_max_udaf;
mod prom_max_udaf_name;
mod prom_min_udaf;
mod prom_min_udaf_name;
mod register_aggregate_udafs;

use extremum::Extremum;
use extremum_udaf::extremum_udaf;
use prom_extremum_accumulator::PromExtremumAccumulator;
pub use prom_max_udaf::prom_max_udaf;
pub use prom_max_udaf_name::PROM_MAX_UDAF_NAME;
pub use prom_min_udaf::prom_min_udaf;
pub use prom_min_udaf_name::PROM_MIN_UDAF_NAME;
pub use register_aggregate_udafs::register_aggregate_udafs;
