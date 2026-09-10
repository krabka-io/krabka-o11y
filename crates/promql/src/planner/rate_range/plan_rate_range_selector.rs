use super::{
    Arc, BTreeSet, Expr, Extension, FunctionRegistry, LabeledSeries, LogicalPlan,
    LogicalPlanBuilder, MemTable, PromqlError, RANGE_SUFFIX, RATE_VALUE_COLUMN, RangeManipulate,
    RateRangePlan, RateUdfKind, Result, SeriesDivide, SeriesNormalize, TIME_COLUMN, Time, TimeExt,
    VALUE_COLUMN, build_leaf_batch, col, leaf_schema, lit, prom_session_context,
};

/// Builds the leaf table and operator chain that evaluates `f(selector[range])`
/// at one eval instant `eval_time_ms` with the given `range` width.
///
/// `series` are the matched series and their float samples over the exact range
/// window `(eval_time_ms - range, eval_time_ms]`, in ascending fingerprint order
/// with each series' samples in timestamp order — which is the contiguous,
/// time-ordered run [`SeriesDivide`] needs. The caller must filter out stale-NaN
/// markers before the values reach the operator chain, which matches the
/// interpreter's `eval_matrix_selector` staleness handling. Genuine NaN values
/// pass through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch, the table, or
/// the projection plan.
pub async fn plan_rate_range_selector(
    series: Vec<LabeledSeries>,
    eval_time_ms: i64,
    range: Time,
    kind: RateUdfKind,
) -> Result<RateRangePlan> {
    // Collect the distinct label names across all matched series; these become
    // the label columns carried through the operator chain and projected out.
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = std::collections::BTreeMap::new();
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
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_rate_leaf", Arc::new(table))?;
    let leaf = ctx.table("prom_rate_leaf").await?.into_optimized_plan()?;

    // SeriesDivide on every label column splits the sorted input into exact
    // per-series batches.
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    // SeriesNormalize sorts each per-series batch by timestamp. The offset is
    // already folded into eval_time_ms by the caller, so it is zero here. NaN is
    // NOT filtered here: matrix selectors keep genuine NaN (only stale-NaN is
    // dropped, which the caller already did), so the operator chain must not
    // strip it.
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    // RangeManipulate folds the samples into the single eval step's window
    // (t - range, t]. A single grid step: start == end == eval_time_ms, and any
    // positive interval covers exactly one point.
    let range_ms = range.millis_i64();
    let range = RangeManipulate::new(
        eval_time_ms,
        eval_time_ms,
        range_ms.max(1),
        range_ms,
        TIME_COLUMN.to_string(),
        VALUE_COLUMN.to_string(),
        normalize,
    )
    .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let range = LogicalPlan::Extension(Extension {
        node: Arc::new(range),
    });

    // Project the label columns through plus the rate-family UDF over the
    // windowed columns, aliased to the result value column.
    let udf = ctx
        .udf(kind.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let time_range_column = format!("{TIME_COLUMN}{RANGE_SUFFIX}");
    let value_range_column = format!("{VALUE_COLUMN}{RANGE_SUFFIX}");
    let rate_call = udf
        .call(vec![
            col(TIME_COLUMN),
            col(time_range_column),
            col(value_range_column),
            lit(range_ms),
        ])
        .alias(RATE_VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(rate_call);

    let plan = LogicalPlanBuilder::from(range)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(RateRangePlan {
        ctx,
        plan,
        labels_by_fp,
    })
}
