use super::{LogqlExpr, ParseError, parse_expr};

/// Parse a complete, recursively nested `LogQL` expression.
///
/// # Errors
///
/// Returns an error when the expression is malformed or contains an unsupported leaf query.
pub fn parse_logql_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    parse_expr(input.trim())
}
