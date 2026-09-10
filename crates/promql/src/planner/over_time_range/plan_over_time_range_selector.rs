use super::{
    Arc, BTreeMap, BTreeSet, Expr, Extension, FunctionRegistry, LabeledSeries, LogicalPlan,
    LogicalPlanBuilder, OVER_TIME_VALUE_COLUMN, OverTimeFamily, OverTimeRangePlan, PromqlError,
    RANGE_SUFFIX, RangeManipulate, Result, SeriesDivide, SeriesNormalize, StepGrid, TIME_COLUMN,
    Time, TimeExt, VALUE_COLUMN, build_leaf_batch, col, leaf_scan, leaf_schema, lit,
    prom_session_context,
};

/// Builds the leaf table and operator chain for `f_over_time(selector[range])`.
///
/// The chain evaluates at every instant of `grid` with the given `range` width,
/// exactly as [`plan_rate_range_selector`](crate::planner::rate_range::plan_rate_range_selector)
/// does. `phi` is the quantile literal for [`OverTimeFamily::Quantile`], and
/// every other family ignores it.
///
/// `series` are the matched series and their float samples over the exact range
/// window `(grid.start - range, grid.end]`, in ascending fingerprint order
/// with each series' samples in timestamp order — which is the contiguous,
/// time-ordered run [`SeriesDivide`] needs. The caller must filter out stale-NaN
/// markers before the values reach the operator chain. Genuine NaN values pass
/// through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch, the table,
/// or the projection plan.
pub async fn plan_over_time_range_selector(
    series: Vec<LabeledSeries>,
    grid: StepGrid,
    range: Time,
    family: OverTimeFamily,
    phi: f64,
) -> Result<OverTimeRangePlan> {
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = BTreeMap::new();
    for one in &series {
        for (name, _) in one.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(one.fp)
            .or_insert_with(|| (*one.labels).clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &series)?;

    let ctx = prom_session_context();
    let leaf = leaf_scan("prom_over_time_leaf", schema, batch)?;

    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    let range_ms = range.millis_i64();
    let range = RangeManipulate::new(
        grid.start,
        grid.end,
        grid.step,
        range_ms,
        TIME_COLUMN.to_string(),
        VALUE_COLUMN.to_string(),
        normalize,
    )
    .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let range = LogicalPlan::Extension(Extension {
        node: Arc::new(range),
    });

    let udf = ctx
        .udf(family.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let time_range_column = format!("{TIME_COLUMN}{RANGE_SUFFIX}");
    let value_range_column = format!("{VALUE_COLUMN}{RANGE_SUFFIX}");

    // `quantile_over_time` threads the `phi` literal ahead of the windowed
    // columns; the other families take only the three windowed columns.
    let mut udf_args: Vec<Expr> = Vec::with_capacity(4);
    if matches!(family, OverTimeFamily::Quantile) {
        udf_args.push(lit(phi));
    }
    udf_args.push(col(TIME_COLUMN));
    udf_args.push(col(time_range_column));
    udf_args.push(col(value_range_column));
    let over_time_call = udf.call(udf_args).alias(OVER_TIME_VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(over_time_call);
    // Carry the eval timestamp through, so a grid-driven plan's output says
    // which instant each row belongs to. See `plan_rate_range_selector`.
    projections.push(col(TIME_COLUMN));

    let plan = LogicalPlanBuilder::from(range)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(OverTimeRangePlan {
        ctx,
        plan,
        labels_by_fp,
    })
}
