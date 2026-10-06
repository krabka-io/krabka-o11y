use super::{LogqlExpr, ParseError, parse_expr, syntax_error};

/// Parse a complete, recursively nested `LogQL` expression.
///
/// # Errors
///
/// Returns an error when the expression is malformed or contains an unsupported leaf query.
pub fn parse_logql_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    let expression = parse_expr(input.trim())?;
    if !matches!(expression, LogqlExpr::Variants { .. }) && expression.contains_variants() {
        return Err(syntax_error("variants must be a top-level expression"));
    }
    Ok(expression)
}
