use std::collections::VecDeque;

use promql_parser::parser::{
    Expr,
    token::{T_GTR, T_LSS, TokenType},
};

// Private AST tokens for operators absent from promql-parser 0.10. Parsing uses
// the corresponding comparison precedence, then restores the operator in order. A temporary bool modifier permits scalar
// operands without treating trim as a comparison. A user bool modifier consequently
// becomes a duplicate and is rejected, as it is for trim in the pinned grammar.
pub(crate) const HISTOGRAM_TRIM_UPPER: u16 = u16::MAX;
pub(crate) const HISTOGRAM_TRIM_LOWER: u16 = u16::MAX - 1;

pub(crate) fn normalize_histogram_trim(query: &str) -> (String, VecDeque<u16>) {
    let mut bytes = query.as_bytes().to_vec();
    let mut operators = VecDeque::new();
    let mut bool_positions = Vec::new();
    let mut quote = None;
    let mut braces = 0_u32;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            if byte == b'\\' && delimiter != b'`' {
                index += 2;
                continue;
            }
            if byte == delimiter {
                quote = None;
            }
        } else if matches!(byte, b'"' | b'\'' | b'`') {
            quote = Some(byte);
        } else if byte == b'#' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        } else if byte == b'{' {
            braces += 1;
        } else if byte == b'}' {
            braces = braces.saturating_sub(1);
        } else if braces == 0 && matches!(byte, b'<' | b'>') {
            let next = bytes.get(index + 1).copied();
            if next == Some(b'/') {
                bytes[index + 1] = b' ';
                bool_positions.push(index + 2);
                operators.push_back(if byte == b'<' {
                    HISTOGRAM_TRIM_UPPER
                } else {
                    HISTOGRAM_TRIM_LOWER
                });
            } else if next != Some(b'=') {
                operators.push_back(if byte == b'<' { T_LSS } else { T_GTR });
            }
        }
        index += 1;
    }
    let mut normalized = String::from_utf8(bytes).expect("only ASCII operator bytes changed");
    for position in bool_positions.into_iter().rev() {
        normalized.insert_str(position, " bool ");
    }
    (normalized, operators)
}

pub(crate) fn restore_histogram_trim(expr: &mut Expr, operators: &mut VecDeque<u16>) {
    match expr {
        Expr::Binary(binary) => {
            restore_histogram_trim(&mut binary.lhs, operators);
            if matches!(binary.op.id(), T_GTR | T_LSS)
                && let Some(operator) = operators.pop_front()
            {
                binary.op = TokenType::new(operator);
                if matches!(operator, HISTOGRAM_TRIM_UPPER | HISTOGRAM_TRIM_LOWER)
                    && let Some(modifier) = &mut binary.modifier
                {
                    modifier.return_bool = false;
                }
            }
            restore_histogram_trim(&mut binary.rhs, operators);
        }
        Expr::Aggregate(aggregate) => {
            if let Some(parameter) = &mut aggregate.param {
                restore_histogram_trim(parameter, operators);
            }
            restore_histogram_trim(&mut aggregate.expr, operators);
        }
        Expr::Call(call) => {
            for argument in &mut call.args.args {
                restore_histogram_trim(argument, operators);
            }
        }
        Expr::Paren(paren) => restore_histogram_trim(&mut paren.expr, operators),
        Expr::Unary(unary) => restore_histogram_trim(&mut unary.expr, operators),
        Expr::Subquery(subquery) => restore_histogram_trim(&mut subquery.expr, operators),
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::Extension(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::parser::{Expr, parse, token::T_GTR};

    use super::{HISTOGRAM_TRIM_UPPER, normalize_histogram_trim, restore_histogram_trim};

    #[test]
    fn trim_preserves_precedence_and_does_not_rewrite_labels_or_comparisons() {
        let (query, mut operators) =
            normalize_histogram_trim(r#"histogram_count(h{label=">/"} </ 1 + 2) > 3"#);
        let mut expr = parse(&query).unwrap();
        restore_histogram_trim(&mut expr, &mut operators);
        let Expr::Binary(outer) = expr else {
            panic!("binary expected")
        };
        assert2::assert!(outer.op.id() == T_GTR);
        let Expr::Call(call) = *outer.lhs else {
            panic!("call expected")
        };
        let Expr::Binary(inner) = call.args.args[0].as_ref() else {
            panic!("trim expected")
        };
        assert2::assert!(inner.op.id() == HISTOGRAM_TRIM_UPPER);
        assert2::assert!(matches!(inner.rhs.as_ref(), Expr::Binary(_)));
        assert2::assert!(operators.is_empty());
    }
}
