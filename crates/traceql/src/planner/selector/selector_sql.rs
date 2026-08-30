use super::*;

pub(crate) fn selector_sql(table: &str, fe: &FieldExpr) -> Result<String> {
    selector_sql_with_parent_table(table, table, fe)
}
