use super::{ScanResult, Token};

struct OperatorToken {
    raw: &'static str,
    token: Token,
}

pub(crate) fn op_token(s: &str) -> Option<ScanResult<Token>> {
    for operator in [
        OperatorToken {
            raw: "!>>",
            token: Token::NegDesc,
        },
        OperatorToken {
            raw: "&>>",
            token: Token::UnionDesc,
        },
        OperatorToken {
            raw: "!<<",
            token: Token::NegAnc,
        },
        OperatorToken {
            raw: "&<<",
            token: Token::UnionAnc,
        },
        OperatorToken {
            raw: ">>",
            token: Token::Desc,
        },
        OperatorToken {
            raw: "<<",
            token: Token::Anc,
        },
        OperatorToken {
            raw: "!>",
            token: Token::NegChild,
        },
        OperatorToken {
            raw: "!<",
            token: Token::NegParent,
        },
        OperatorToken {
            raw: "&>",
            token: Token::UnionChild,
        },
        OperatorToken {
            raw: "&<",
            token: Token::UnionParent,
        },
        OperatorToken {
            raw: "&~",
            token: Token::UnionSibling,
        },
        OperatorToken {
            raw: "&&",
            token: Token::And,
        },
        OperatorToken {
            raw: "||",
            token: Token::Or,
        },
        OperatorToken {
            raw: ">=",
            token: Token::Gte,
        },
        OperatorToken {
            raw: "<=",
            token: Token::Lte,
        },
        OperatorToken {
            raw: "=~",
            token: Token::Re,
        },
        OperatorToken {
            raw: "!~",
            token: Token::NegSibling,
        },
        OperatorToken {
            raw: "!=",
            token: Token::Neq,
        },
    ] {
        if s.starts_with(operator.raw) {
            return Some(ScanResult {
                value: operator.token,
                consumed: operator.raw.len(),
            });
        }
    }

    let ch = s.chars().next()?;
    let tok = match ch {
        '=' => Token::Eq,
        '<' => Token::Parent,
        '>' => Token::Child,
        '~' => Token::Sibling,
        '!' => Token::Not,
        '+' => Token::Plus,
        '-' => Token::Minus,
        '*' => Token::Star,
        '/' => Token::Slash,
        '%' => Token::Mod,
        '^' => Token::Caret,
        '.' => Token::Dot,
        ':' => Token::Colon,
        ',' => Token::Comma,
        '(' => Token::LParen,
        ')' => Token::RParen,
        '{' => Token::LBrace,
        '}' => Token::RBrace,
        '|' => Token::Pipe,
        '&' => {
            return Some(ScanResult {
                value: Token::Ident("&".into()),
                consumed: ch.len_utf8(),
            });
        }
        _ => return None,
    };
    Some(ScanResult {
        value: tok,
        consumed: ch.len_utf8(),
    })
}
