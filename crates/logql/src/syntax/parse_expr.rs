use super::{
    ExprOperator, LogqlExpr, ParseError, Parser, operator_at, parse_expr_primary, parse_query,
    scan_top_level, syntax_error,
};

struct LocatedOperator {
    start: usize,
    token_length: usize,
    operator: ExprOperator,
    precedence: u8,
}

pub(crate) fn parse_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(syntax_error("expected expression"));
    }
    let stream_error = if input.starts_with('{') {
        match parse_query(input) {
            Ok(query) => {
                return Ok(LogqlExpr::Stream {
                    query,
                    source: input.to_string(),
                });
            }
            Err(error) => Some(error),
        }
    } else {
        None
    };
    let mut candidate: Option<LocatedOperator> = None;
    if let Err(error) = scan_top_level(input, |at| {
        if let Some(found) = operator_at(input, at)
            && candidate.as_ref().is_none_or(|old| {
                found.precedence < old.precedence
                    || (found.precedence == old.precedence && found.precedence != 6)
            })
        {
            candidate = Some(LocatedOperator {
                start: at,
                token_length: found.token_length,
                operator: found.operator,
                precedence: found.precedence,
            });
        }
    }) {
        return Err(stream_error.unwrap_or(error));
    }
    if let Some(candidate) = candidate {
        let left = parse_expr(&input[..candidate.start])?;
        let mut parser = Parser::new(&input[candidate.start + candidate.token_length..]);
        let bool_modifier = matches!(candidate.operator, ExprOperator::Comparison(_))
            && parser.consume_keyword("bool");
        let matching = parser.parse_metric_vector_matching_modifier(!matches!(
            candidate.operator,
            ExprOperator::Set(_)
        ))?;
        let right_text = &parser.input[parser.pos..];
        let right = parse_expr(right_text)?;
        return Ok(match candidate.operator {
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
    match parse_expr_primary(input) {
        Ok(expression) => Ok(expression),
        Err(error) => Err(stream_error.unwrap_or(error)),
    }
}
