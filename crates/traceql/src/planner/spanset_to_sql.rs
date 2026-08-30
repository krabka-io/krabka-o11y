use super::*;

pub(crate) fn spanset_to_sql(
    expr: &SpansetExpr,
    table: &str,
    nested_tables: &[(FieldExpr, String)],
) -> Result<String> {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            let Some((_, table_name)) = nested_tables
                .iter()
                .find(|(candidate, _)| candidate == fe.as_ref())
            else {
                return Err(TraceqlError::Plan(
                    "nested selector table was not registered".into(),
                ));
            };
            let nested_table = selector::ident(table_name);
            if selector::has_parent_scope(fe) {
                selector::selector_sql_with_parent_table(&nested_table, table, fe)
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
