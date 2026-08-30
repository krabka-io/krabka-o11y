use super::*;

pub(crate) fn op_token(s: &str) -> Option<(Token, usize)> {
    for (raw, tok) in [
        ("!>>", Token::NegDesc),
        ("&>>", Token::UnionDesc),
        ("!<<", Token::NegAnc),
        ("&<<", Token::UnionAnc),
        (">>", Token::Desc),
        ("<<", Token::Anc),
        ("!>", Token::NegChild),
        ("!<", Token::NegParent),
        ("&>", Token::UnionChild),
        ("&<", Token::UnionParent),
        ("&~", Token::UnionSibling),
        ("&&", Token::And),
        ("||", Token::Or),
        (">=", Token::Gte),
        ("<=", Token::Lte),
        ("=~", Token::Re),
        ("!~", Token::Nre),
        ("!=", Token::Neq),
    ] {
        if s.starts_with(raw) {
            return Some((tok, raw.len()));
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
            return Some((Token::Ident("&".into()), ch.len_utf8()));
        }
        _ => return None,
    };
    Some((tok, ch.len_utf8()))
}
