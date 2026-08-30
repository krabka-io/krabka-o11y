use super::*;

pub(crate) fn keyword_or_ident(s: String) -> Token {
    match s.as_str() {
        "nil" => Token::Nil,
        "true" => Token::Bool(true),
        "false" => Token::Bool(false),
        _ => Token::Ident(s),
    }
}
