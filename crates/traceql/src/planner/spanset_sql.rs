use super::registration::NestedSelectorTable;
use super::{
    COL_SPAN_ID, COL_TRACE_ID, Pipeline, PlannedSpanset, PlannerContext,
    Result, SpanStore, SpansetExpr, TraceqlError, pipeline_to_sql,
    register_nested_selector_tables, scan_options_with_pipeline_projections, selector,
    structural_base_op, structural_is_negated, structural_is_union, structural_predicate_sql,
};

pub(crate) fn spanset_to_sql(
    expr: &SpansetExpr,
    table: &str,
    nested_tables: &[NestedSelectorTable],
) -> Result<String> {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            let Some(nested_selector_table) = nested_tables
                .iter()
                .find(|candidate| candidate.selector == *fe.as_ref())
            else {
                return Err(TraceqlError::Plan(
                    "nested selector table was not registered".into(),
                ));
            };
            let nested_table = selector::ident(&nested_selector_table.table_name);
            if selector::has_parent_scope(fe) {
                selector::selector_sql_with_parent_table(&nested_table, table, fe)
            } else if fe.has_field_comparison() {
                selector::selector_sql(&nested_table, fe)
            } else {
                Ok(format!("SELECT * FROM {nested_table}"))
            }
        }
        SpansetExpr::Selector(fe) => selector::selector_sql(table, fe),
        SpansetExpr::Or(lhs, rhs) => Ok(format!(
            "({}) UNION ({})",
            spanset_to_sql(lhs, table, nested_tables)?,
            spanset_to_sql(rhs, table, nested_tables)?
        )),
        SpansetExpr::And(lhs, rhs) => {
            let l = spanset_to_sql(lhs, table, nested_tables)?;
            let r = spanset_to_sql(rhs, table, nested_tables)?;
            let trace = selector::ident(COL_TRACE_ID);
            Ok(format!(
                "(SELECT l.* FROM ({l}) AS l WHERE EXISTS (SELECT 1 FROM ({r}) AS r WHERE r.{trace} = l.{trace})) \
                 UNION \
                 (SELECT r.* FROM ({r}) AS r WHERE EXISTS (SELECT 1 FROM ({l}) AS l WHERE l.{trace} = r.{trace}))"
            ))
        }
        SpansetExpr::Structural { op, lhs, rhs } => {
            let b = spanset_to_sql(rhs, table, nested_tables)?;
            let a = spanset_to_sql(lhs, table, nested_tables)?;
            let pred = structural_predicate_sql(structural_base_op(*op));
            if structural_is_negated(*op) {
                return Ok(format!(
                    "SELECT DISTINCT b.* FROM ({b}) AS b LEFT JOIN ({a}) AS a ON {pred} \
                     WHERE a.{} IS NULL",
                    selector::ident(COL_SPAN_ID)
                ));
            }
            if structural_is_union(*op) {
                return Ok(format!(
                    "(SELECT DISTINCT b.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred}) \
                     UNION \
                     (SELECT DISTINCT a.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred})"
                ));
            }
            Ok(format!(
                "SELECT DISTINCT b.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred}"
            ))
        }
    }
}

pub(crate) async fn plan_spanset_sql<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    root: &SpansetExpr,
    pipeline: &[Pipeline],
) -> Result<PlannedSpanset> {
    let scan_options = scan_options_with_pipeline_projections(&ctx.scan_options, pipeline);
    let scan = store
        .scan_with_options(
            &ctx.tenant,
            &[],
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &scan_options,
        )
        .await?;
    let sampled =
        super::sample_metric_scan::sample_metric_scan(&scan.ctx, &scan.span_table, &scan_options)
            .await?;
    let span_table = sampled.table;
    let sampling_factor = sampled.scale;
    let mut selectors = Vec::new();
    super::registration::collect_field_selectors(root, &mut selectors);
    super::registration::register_field_comparison_columns(&scan.ctx, &span_table, &selectors).await?;
    let inspected = scan.inspected;
    let nested_tables = register_nested_selector_tables(store, ctx, &scan.ctx, root).await?;
    let spanset_sql = spanset_to_sql(root, &selector::ident(&span_table), &nested_tables)?;
    if super::apply_expression_pipeline::uses_expression_pipeline(pipeline) {
        let batches = scan.ctx.sql(&spanset_sql).await?.collect().await?;
        let spanset_pipeline_had_input = batches.iter().any(|batch| batch.num_rows() > 0);
        let batches =
            super::apply_expression_pipeline::apply_expression_pipeline(&batches, pipeline)?;
        super::register_batches(&scan.ctx, "expression_pipeline", batches)?;
        return Ok(PlannedSpanset {
            plan: scan
                .ctx
                .table("expression_pipeline")
                .await?
                .into_unoptimized_plan(),
            ctx: scan.ctx,
            inspected,
            sampling_factor,
            spanset_pipeline_had_input,
        });
    }
    let sql = pipeline_to_sql(&spanset_sql, pipeline)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected,
        sampling_factor,
        spanset_pipeline_had_input: false,
    })
}
