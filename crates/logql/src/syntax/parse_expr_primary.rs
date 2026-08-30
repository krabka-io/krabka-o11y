use super::{LogqlExpr, ParseError, outer_metric_parentheses_inner, parse_expr, function_args, syntax_error, parse_string_arg, parse_scalar_text, parse_metric_query};

pub(crate) fn parse_expr_primary(input: &str) -> Result<LogqlExpr, ParseError> {
    if let Some(inner) = outer_metric_parentheses_inner(input) {
        return parse_expr(inner);
    }
    for (name, descending) in [("sort_desc", true), ("sort", false)] {
        if let Some(args) = function_args(input, name)? {
            if args.len() != 1 {
                return Err(syntax_error("expected one function argument"));
            }
            return Ok(LogqlExpr::Sort {
                expr: Box::new(parse_expr(args[0])?),
                descending,
            });
        }
    }
    if let Some(args) = function_args(input, "vector")? {
        if args.len() != 1 {
            return Err(syntax_error("expected one function argument"));
        }
        let expr = parse_expr(args[0])?;
        if !expr.is_scalar() {
            return Err(syntax_error("vector argument must be scalar"));
        }
        return Ok(LogqlExpr::Vector(Box::new(expr)));
    }
    if let Some(args) = function_args(input, "label_replace")? {
        if args.len() != 5 {
            return Err(syntax_error("expected five function arguments"));
        }
        return Ok(LogqlExpr::LabelReplace {
            expr: Box::new(parse_expr(args[0])?),
            destination_label: parse_string_arg(args[1])?,
            replacement: parse_string_arg(args[2])?,
            source_label: parse_string_arg(args[3])?,
            pattern: parse_string_arg(args[4])?,
        });
    }
    if let Some(args) = function_args(input, "label_join")? {
        if args.len() < 4 {
            return Err(syntax_error("expected at least four function arguments"));
        }
        return Ok(LogqlExpr::LabelJoin {
            expr: Box::new(parse_expr(args[0])?),
            destination_label: parse_string_arg(args[1])?,
            separator: parse_string_arg(args[2])?,
            source_labels: args[3..]
                .iter()
                .map(|arg| parse_string_arg(arg))
                .collect::<Result<_, _>>()?,
        });
    }
    if parse_scalar_text(input) {
        return Ok(LogqlExpr::Scalar(input.to_string()));
    }
    Ok(LogqlExpr::Metric {
        query: parse_metric_query(input)?,
        source: input.to_string(),
    })
}
