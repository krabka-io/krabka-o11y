use super::*;

pub(crate) fn parse_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(syntax_error("expected expression"));
    }
    if input.starts_with('{')
        && let Ok(query) = parse_query(input)
    {
        return Ok(LogqlExpr::Stream {
            query,
            source: input.to_string(),
        });
    }
    let mut candidate = None;
    scan_top_level(input, |at| {
        if let Some((len, kind, precedence)) = operator_at(input, at)
            && candidate.as_ref().is_none_or(|(_, _, _, old)| {
                precedence < *old || (precedence == *old && precedence != 6)
            })
        {
            candidate = Some((at, len, kind, precedence));
        }
    })?;
    if let Some((at, len, kind, _precedence)) = candidate {
        let left = parse_expr(&input[..at])?;
        let mut parser = Parser::new(&input[at + len..]);
        let bool_modifier =
            matches!(kind, ExprOperator::Comparison(_)) && parser.consume_keyword("bool");
        let matching =
            parser.parse_metric_vector_matching_modifier(!matches!(kind, ExprOperator::Set(_)))?;
        let right_text = &parser.input[parser.pos..];
        let right = parse_expr(right_text)?;
        return Ok(match kind {
            ExprOperator::Arithmetic(op) => LogqlExpr::Arithmetic {
                left: Box::new(left),
                op,
                matching,
                right: Box::new(right),
            },
            ExprOperator::Comparison(op) => LogqlExpr::Comparison {
                left: Box::new(left),
                op,
                bool_modifier,
                matching,
                right: Box::new(right),
            },
            ExprOperator::Set(op) => LogqlExpr::Set {
                left: Box::new(left),
                op,
                matching,
                right: Box::new(right),
            },
        });
    }
    parse_expr_primary(input)
}
