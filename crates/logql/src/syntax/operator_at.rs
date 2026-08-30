use super::{ExprOperator, is_ident_char, MetricBinarySetOp, ComparisonOp, sign_is_unary_or_exponent, MetricScalarArithmeticOp};

pub(crate) fn operator_at(input: &str, at: usize) -> Option<(usize, ExprOperator, u8)> {
    let rest = &input[at..];
    let boundary = |n: usize| {
        input[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !is_ident_char(c))
            && input[at + n..]
                .chars()
                .next()
                .is_none_or(|c| !is_ident_char(c))
    };
    for (word, op, precedence) in [
        ("unless", MetricBinarySetOp::Unless, 2),
        ("and", MetricBinarySetOp::And, 2),
        ("or", MetricBinarySetOp::Or, 1),
    ] {
        if rest.starts_with(word) && boundary(word.len()) {
            return Some((word.len(), ExprOperator::Set(op), precedence));
        }
    }
    for (text, op) in [
        (">=", ComparisonOp::GreaterEqual),
        ("<=", ComparisonOp::LessEqual),
        ("==", ComparisonOp::Equal),
        ("!=", ComparisonOp::NotEqual),
        (">", ComparisonOp::Greater),
        ("<", ComparisonOp::Less),
    ] {
        if rest.starts_with(text) {
            return Some((text.len(), ExprOperator::Comparison(op), 3));
        }
    }
    let ch = rest.chars().next()?;
    if matches!(ch, '+' | '-') && sign_is_unary_or_exponent(input, at) {
        return None;
    }
    let (op, p) = match ch {
        '+' => (MetricScalarArithmeticOp::Add, 4),
        '-' => (MetricScalarArithmeticOp::Subtract, 4),
        '*' => (MetricScalarArithmeticOp::Multiply, 5),
        '/' => (MetricScalarArithmeticOp::Divide, 5),
        '%' => (MetricScalarArithmeticOp::Modulo, 5),
        '^' => (MetricScalarArithmeticOp::Power, 6),
        _ => return None,
    };
    Some((1, ExprOperator::Arithmetic(op), p))
}
