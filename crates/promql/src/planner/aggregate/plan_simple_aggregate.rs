use super::*;

/// Wraps `input` in a `DataFusion` aggregate for `op grouping (<input>)`.
///
/// The aggregate implements that `PromQL` aggregation. `input` is the inner
/// planner plan. The `Utf8` columns of its output schema are the candidate
/// grouping labels: every column except the `value`/`timestamp`/
/// `sample_timestamp` index columns. The output of the returned plan is the
/// surviving grouping label columns plus the aggregated `value` column.
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] if this function cannot build the aggregate
/// plan.
pub fn plan_simple_aggregate(
    input: LogicalPlan,
    op: SimpleAggregateOp,
    grouping: &Grouping,
) -> Result<LogicalPlan> {
    let input_labels = input_label_columns(&input);
    let group_labels = resolve_group_labels(&input_labels, grouping);

    // Drop no-value input rows (a NULL `value`, the rate/`*_over_time` UDF's
    // "no value" marker) before grouping. The interpreter omits no-value series
    // from a group entirely, so a group whose members are all no-value forms no
    // result row at all, and `count` counts only value-bearing series. Filtering
    // here reproduces that exactly: such rows never reach the aggregate, so the
    // group either disappears (all no-value) or aggregates over its value-bearing
    // members only. A genuine NaN value is non-null, so it survives the filter and
    // propagates (e.g. `sum` over a group holding a genuine NaN yields NaN). For a
    // selector inner (whose `value` is non-nullable) the filter is a no-op.
    let input = LogicalPlanBuilder::from(input)
        .filter(col(VALUE_COLUMN).is_not_null())
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // When there is no grouping label, group by a synthetic constant-valued
    // per-row column so an empty input yields zero groups (matching Prometheus'
    // empty vector) rather than SQL's single global-aggregate row. The column is
    // projected away in the final result. Only the `value` column is needed
    // downstream (the aggregate reads it; `group` ignores it), so the synthetic
    // projection carries just `value` plus the group column.
    let (input, group_exprs): (LogicalPlan, Vec<Expr>) = if group_labels.is_empty() {
        let projected = LogicalPlanBuilder::from(input)
            .project(vec![col(VALUE_COLUMN), lit("").alias(ALL_GROUP_COLUMN)])
            .map_err(|error| PromqlError::Exec(error.to_string()))?
            .build()
            .map_err(|error| PromqlError::Exec(error.to_string()))?;
        (projected, vec![col(ALL_GROUP_COLUMN)])
    } else {
        (input, group_labels.iter().map(col).collect())
    };
    let aggr_exprs = vec![op.value_aggregate()];

    let aggregated = LogicalPlanBuilder::from(input)
        .aggregate(group_exprs, aggr_exprs)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // Project the grouping labels through plus the value column. This pins the
    // column order the engine's batch reader expects, keeps the value column
    // last, and drops the synthetic all-group column when one was injected.
    let mut projections: Vec<Expr> = group_labels.iter().map(col).collect();
    projections.push(col(AGGREGATE_VALUE_COLUMN));
    LogicalPlanBuilder::from(aggregated)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))
}
