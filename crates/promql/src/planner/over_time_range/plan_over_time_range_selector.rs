use super::{LabeledSample, Time, OverTimeFamily, Result, OverTimeRangePlan, BTreeSet, BTreeMap, leaf_schema, build_leaf_batch, Arc, prom_session_context, MemTable, PromqlError, LogicalPlan, Extension, SeriesDivide, SeriesNormalize, TIME_COLUMN, TimeExt, RangeManipulate, VALUE_COLUMN, FunctionRegistry, RANGE_SUFFIX, Expr, lit, col, OVER_TIME_VALUE_COLUMN, LogicalPlanBuilder};

/// Builds the leaf table and operator chain for `f_over_time(selector[range])`.
///
/// The chain evaluates at one eval instant `eval_time_ms` with the given
/// `range` width. `phi` is the quantile literal for
/// [`OverTimeFamily::Quantile`], and every other family ignores it.
///
/// `samples` are the float samples of the matched series over the exact range
/// window `(eval_time_ms - range, eval_time_ms]`. The caller must filter out
/// stale-NaN markers before the values reach the operator chain. Genuine NaN
/// values pass through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch, the table,
/// or the projection plan.
pub async fn plan_over_time_range_selector(
    samples: Vec<LabeledSample>,
    eval_time_ms: i64,
    range: Time,
    family: OverTimeFamily,
    phi: f64,
) -> Result<OverTimeRangePlan> {
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = BTreeMap::new();
    for sample in &samples {
        for (name, _) in sample.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(sample.fp)
            .or_insert_with(|| sample.labels.clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let mut rows = samples;
    rows.sort_by(|left, right| {
        left.fp
            .cmp(&right.fp)
            .then_with(|| left.ts_ms.cmp(&right.ts_ms))
    });

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &rows)?;

    let ctx = prom_session_context();
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_over_time_leaf", Arc::new(table))?;
    let leaf = ctx
        .table("prom_over_time_leaf")
        .await?
        .into_optimized_plan()?;

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
