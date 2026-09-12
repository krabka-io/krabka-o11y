use super::{
    LogqlExpr, ParseError, function_args, outer_metric_parentheses_inner, parse_expr,
    parse_metric_query, parse_scalar_text, parse_string_arg, syntax_error,
};

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
    for (name, largest, approximate) in [
        ("topk", true, false),
        ("bottomk", false, false),
        ("approx_topk", true, true),
    ] {
        if let Some(args) = function_args(input, name)? {
            if let Ok(query) = parse_metric_query(input) {
                return Ok(LogqlExpr::Metric {
                    query,
                    source: input.to_string(),
                });
            }
            if args.len() != 2 {
                return Err(syntax_error("expected selection limit and expression"));
            }
            let limit = args[0]
                .trim()
                .parse()
                .map_err(|_| syntax_error("expected integer selection limit"))?;
            return Ok(LogqlExpr::Selection {
                expr: Box::new(parse_expr(args[1])?),
                limit,
                largest,
                approximate,
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
