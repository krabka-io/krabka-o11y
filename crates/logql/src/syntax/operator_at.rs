use super::{
    ComparisonOp, ExprOperator, MetricBinarySetOp, MetricScalarArithmeticOp, is_ident_char,
    sign_is_unary_or_exponent,
};

#[derive(Clone, Copy)]
pub(crate) struct OperatorMatch {
    pub(crate) token_length: usize,
    pub(crate) operator: ExprOperator,
    pub(crate) precedence: u8,
}

struct WordOperatorCase {
    word: &'static str,
    op: MetricBinarySetOp,
    precedence: u8,
}

struct ComparisonOperatorCase {
    text: &'static str,
    op: ComparisonOp,
}

struct ArithmeticOperatorMatch {
    operator: MetricScalarArithmeticOp,
    precedence: u8,
}

pub(crate) fn operator_at(input: &str, at: usize) -> Option<OperatorMatch> {
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
    for case in [
        WordOperatorCase {
            word: "unless",
            op: MetricBinarySetOp::Unless,
            precedence: 2,
        },
        WordOperatorCase {
            word: "and",
            op: MetricBinarySetOp::And,
            precedence: 2,
        },
        WordOperatorCase {
            word: "or",
            op: MetricBinarySetOp::Or,
            precedence: 1,
        },
    ] {
        if rest.starts_with(case.word) && boundary(case.word.len()) {
            return Some(OperatorMatch {
                token_length: case.word.len(),
                operator: ExprOperator::Set(case.op),
                precedence: case.precedence,
            });
        }
    }
    for case in [
        ComparisonOperatorCase {
            text: ">=",
            op: ComparisonOp::GreaterEqual,
        },
        ComparisonOperatorCase {
            text: "<=",
            op: ComparisonOp::LessEqual,
        },
        ComparisonOperatorCase {
            text: "==",
            op: ComparisonOp::Equal,
        },
        ComparisonOperatorCase {
            text: "!=",
            op: ComparisonOp::NotEqual,
        },
        ComparisonOperatorCase {
            text: ">",
            op: ComparisonOp::Greater,
        },
        ComparisonOperatorCase {
            text: "<",
            op: ComparisonOp::Less,
        },
    ] {
        if rest.starts_with(case.text) {
            return Some(OperatorMatch {
                token_length: case.text.len(),
                operator: ExprOperator::Comparison(case.op),
                precedence: 3,
            });
        }
    }
    let ch = rest.chars().next()?;
    if matches!(ch, '+' | '-') && sign_is_unary_or_exponent(input, at) {
        return None;
    }
    let matched = match ch {
        '+' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Add,
            precedence: 4,
        },
        '-' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Subtract,
            precedence: 4,
        },
        '*' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Multiply,
            precedence: 5,
        },
        '/' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Divide,
            precedence: 5,
        },
        '%' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Modulo,
            precedence: 5,
        },
        '^' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Power,
            precedence: 6,
        },
        _ => return None,
    };
    Some(OperatorMatch {
        token_length: 1,
        operator: ExprOperator::Arithmetic(matched.operator),
        precedence: matched.precedence,
    })
}
