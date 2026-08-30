use super::{
    Result, Token, TraceqlError, advance, is_ident_start, keyword_or_ident, no_progress, op_token,
    scan_ident, scan_number_or_duration, scan_string,
};

/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
/// # Panics
/// Panics if a parsed expression or span set violates an invariant established during `TraceQL` validation.
pub fn lex(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut i = 0;
    // Only one thing about the previous token matters: whether it was a
    // dot, which is what lets an identifier carry dots of its own.
    let mut after_dot = false;

    while i < input.len() {
        let rest = &input[i..];
        let ch = rest.chars().next().unwrap();
        if ch.is_whitespace() {
            let next = advance(input, i, ch.len_utf8())?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            continue;
        }

        if rest.starts_with("==") {
            return Err(TraceqlError::Parse(format!(
                "use single = for equality; == is not TraceQL at byte {i}"
            )));
        }

        // A `.` immediately followed by a digit (e.g. `.05`, `.99`) is a
        // leading-dot fractional number, lexed as a single `Token::Float` so
        // leading zeros survive. A `.` followed by an identifier (e.g.
        // `.service`) remains a `Dot` for attribute-scope syntax.
        if ch == '.'
            && rest[1..]
                .chars()
                .next()
                .is_some_and(|next| next.is_ascii_digit())
        {
            let (tok, len) = scan_number_or_duration(rest)?;
            tokens.push(tok);
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        if let Some((tok, len)) = op_token(rest) {
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = tok == Token::Dot;
            tokens.push(tok);
            continue;
        }

        if ch == '"' {
            let (s, len) = scan_string(rest)?;
            tokens.push(Token::Str(s));
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        if ch.is_ascii_digit() {
            let (tok, len) = scan_number_or_duration(rest)?;
            tokens.push(tok);
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        if is_ident_start(ch) {
            let allow_dots = after_dot;
            let (ident, len) = scan_ident(rest, allow_dots);
            tokens.push(keyword_or_ident(ident));
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        return Err(TraceqlError::Parse(format!(
            "unexpected character {ch:?} at byte {i}"
        )));
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}
