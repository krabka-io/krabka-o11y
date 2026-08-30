use super::{LabeledValue, ScalarMathOp, Result, ScalarMathPlan, BTreeSet, Labels, is_metadata_label, leaf_schema, build_leaf_batch, Arc, prom_session_context, MemTable, PromqlError, FunctionRegistry, Expr, lit, col, VALUE_COLUMN, SAMPLE_TIME_COLUMN, LogicalPlanBuilder};

/// Builds the leaf table and projection that evaluates `op([bounds...,] value)`.
///
/// This function evaluates the operator over the already-evaluated inner instant
/// vector `samples`. `bounds` holds the leading scalar arguments in call order:
/// `[]` for the unary functions, `[to_nearest]` for `round`, `[min]` for
/// `clamp_min`, `[max]` for `clamp_max`, and `[min, max]` for `clamp`.
///
/// # Errors
///
/// This function returns an error if it cannot build the Arrow batch, the table,
/// or the projection plan.
pub async fn plan_scalar_math(
    samples: Vec<LabeledValue>,
    op: ScalarMathOp,
    bounds: &[f64],
) -> Result<ScalarMathPlan> {
    // Collect the distinct non-metadata label names; these become the leaf's
    // label columns. The series fingerprint is recomputed over the metadata-free
    // label set so it matches the projected output exactly.
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut rows: Vec<(Labels, i64, f64)> = Vec::with_capacity(samples.len());
    for sample in samples {
        let mut labels = Labels::new();
        for (name, value) in sample.labels.iter() {
            if !is_metadata_label(name) {
                label_names.insert(name.clone());
                labels.insert(name, value);
            }
        }
        rows.push((labels, sample.ts_ms, sample.value));
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &rows)?;

    let ctx = prom_session_context();
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_scalar_math_leaf", Arc::new(table))?;
    let leaf = ctx
        .table("prom_scalar_math_leaf")
        .await?
        .into_optimized_plan()?;

    let udf = ctx
        .udf(op.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    // The UDF call threads the constant scalar bounds ahead of the value column,
    // matching the scalar-math call convention.
    let mut udf_args: Vec<Expr> = bounds.iter().map(|bound| lit(*bound)).collect();
    udf_args.push(col(VALUE_COLUMN));
    let call = udf.call(udf_args).alias(VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(call);
    // Carry the inner sample timestamp through unchanged so the assembler can
    // report it (the scalar-math functions preserve `sample.ts_ms`).
    projections.push(col(SAMPLE_TIME_COLUMN));

    let plan = LogicalPlanBuilder::from(leaf)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(ScalarMathPlan { ctx, plan })
}
