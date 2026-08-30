use super::*;

pub(crate) fn selector_sql_with_parent_table(
    table: &str,
    parent_table: &str,
    fe: &FieldExpr,
) -> Result<String> {
    if has_nested_scope(fe) {
        if has_parent_scope(fe)
            && let Some(predicate) = parent_field_expr_to_sql_qualified(fe, "s", "p")?
        {
            let trace = ident(COL_TRACE_ID);
            let parent = ident(COL_PARENT_ID);
            let left = ident(COL_NS_LEFT);
            return Ok(format!(
                "SELECT s.* FROM {table} AS s JOIN {parent_table} AS p \
                 ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
                 WHERE {predicate}"
            ));
        }
        return Ok(format!("SELECT * FROM {table}"));
    }
    if has_parent_scope(fe) {
        let predicate = field_expr_to_sql_qualified(fe, "s", "p")?;
        let trace = ident(COL_TRACE_ID);
        let parent = ident(COL_PARENT_ID);
        let left = ident(COL_NS_LEFT);
        Ok(format!(
            "SELECT s.* FROM {table} AS s JOIN {table} AS p \
             ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
             WHERE {predicate}"
        ))
    } else {
        let predicate = field_expr_to_sql(fe)?;
        Ok(format!("SELECT * FROM {table} WHERE {predicate}"))
    }
}
