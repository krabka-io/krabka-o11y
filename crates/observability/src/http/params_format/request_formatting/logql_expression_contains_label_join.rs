use super::*;

pub(crate) fn logql_expression_contains_label_join(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::LabelJoin { .. } => true,
        LogqlExpr::Vector(expression)
        | LogqlExpr::LabelReplace {
            expr: expression, ..
        }
        | LogqlExpr::Sort {
            expr: expression, ..
        } => logql_expression_contains_label_join(expression),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => {
            logql_expression_contains_label_join(left)
                || logql_expression_contains_label_join(right)
        }
        LogqlExpr::Stream { .. } | LogqlExpr::Metric { .. } | LogqlExpr::Scalar(_) => false,
    }
}
