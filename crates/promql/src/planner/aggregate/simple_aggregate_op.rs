use super::{
    AGGREGATE_VALUE_COLUMN, Expr, VALUE_COLUMN, avg, cast, col, count, lit, max, prom_max_udaf,
    prom_min_udaf, sum,
};

/// The simple aggregation operators this module lowers.
///
/// The param ops `topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`
/// are out of scope and never reach here. The recursive planner returns
/// `Unsupported` for them, so the interpreter owns them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimpleAggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
}

impl SimpleAggregateOp {
    /// Builds the per-group value aggregate expression over the `value` column.
    ///
    /// The expression is aliased to the output value column. `count` is cast to
    /// `Float64`, because Prometheus reports counts as floats. `group` is the
    /// constant `1.0` for each group.
    pub(crate) fn value_aggregate(self) -> Expr {
        let value = col(VALUE_COLUMN);
        match self {
            Self::Sum => sum(value),
            Self::Avg => avg(value),
            // Arrow's built-in min/max propagate NaN (total_cmp ordering);
            // Prometheus ignores it. Lower onto the NaN-ignoring UDAFs so the
            // operator path matches the interpreter bit-for-bit, including the
            // all-NaN -> NaN case.
            Self::Min => prom_min_udaf().call(vec![value]),
            Self::Max => prom_max_udaf().call(vec![value]),
            // COUNT yields Int64 in DataFusion; Prometheus reports a float.
            Self::Count => cast(count(value), arrow::datatypes::DataType::Float64),
            // `group` ignores the values entirely and emits 1.0 per group.
            // `max(1.0)` over a non-empty group is exactly 1.0 and needs no
            // value column, keeping the aggregate valid even when `value` is NaN.
            Self::Group => max(lit(1.0_f64)),
        }
        .alias(AGGREGATE_VALUE_COLUMN)
    }
}
