use std::convert::Infallible;

use promql_parser::{
    parser::Expr,
    util::{ExprVisitor, walk_expr},
};

use super::emit_warning;

struct RangeSortWarnings;

impl ExprVisitor for RangeSortWarnings {
    type Error = Infallible;

    fn pre_visit(&mut self, expr: &Expr) -> Result<bool, Self::Error> {
        if let Expr::Call(call) = expr
            && matches!(
                call.func.name,
                "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc"
            )
        {
            emit_warning("PromQL warning: sort is ineffective for range queries since results are always ordered by labels".to_string());
        }
        Ok(true)
    }
}

pub(crate) fn emit_range_sort_warnings(expr: &Expr) {
    let _ = walk_expr(&mut RangeSortWarnings, expr);
}
