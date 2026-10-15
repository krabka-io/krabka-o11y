//! `TraceQL` lexer.

use crate::error::{Result, TraceqlError};

pub(crate) struct ScanResult<T> {
    pub(crate) value: T,
    pub(crate) consumed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Token> {
        let mut t = lex(s).unwrap();
        assert2::assert!(t.pop() == Some(Token::Eof));
        t
    }

    /// A decimal point is only part of a number when a digit follows it, and
    /// only the first one is. Everything else is a separate token, so the
    /// scanner has to stop in the right place rather than swallow the rest.
    #[test]
    fn a_decimal_point_belongs_to_a_number_only_when_a_digit_follows() {
        assert2::assert!(toks("1") == vec![Token::Int(1)]);
        assert2::assert!(toks("1.5") == vec![Token::Float(1.5)]);
        assert2::assert!(toks("0.25") == vec![Token::Float(0.25)]);
        assert2::assert!(
            toks("10.75") == vec![Token::Float(10.75)],
            "more than one digit each side"
        );

        // A second point is not part of the number, so it ends there.
        assert2::assert!(
            toks("1.5.5") == vec![Token::Float(1.5), Token::Float(0.5)],
            "the first point wins and the rest lexes on its own"
        );

        // A point with no digit after it is not part of the number at all.
        assert2::assert!(toks("1 .5") == vec![Token::Int(1), Token::Float(0.5)]);
        assert2::assert!(
            toks("1.") == vec![Token::Int(1), Token::Dot],
            "a trailing point ends the number rather than joining it"
        );
    }

    #[test]
    fn single_equals_no_double() {
        assert2::assert!(
            toks(".http.status = 200")
                == vec![
                    Token::Dot,
                    Token::Ident("http.status".into()),
                    Token::Eq,
                    Token::Int(200),
                ]
        );
        assert2::assert!(lex("a == b").is_err());
    }

    #[yare::parameterized(
        descendant = { "a >> b", Token::Desc },
        negated_descendant = { "a !>> b", Token::NegDesc },
        union_descendant = { "a &>> b", Token::UnionDesc },
        negated_child = { "a !> b", Token::NegChild },
        union_sibling = { "a &~ b", Token::UnionSibling },
    )]
    fn structural_maximal_munch(input: &str, middle: Token) {
        assert2::assert!(
            toks(input) == vec![Token::Ident("a".into()), middle, Token::Ident("b".into())],
            "input: {input}"
        );
    }

    #[yare::parameterized(
        regex_match = { "x =~ \"a.*\"", vec![Token::Ident("x".into()), Token::Re, Token::Str("a.*".into())] },
        regex_negation = { "x !~ \"a\"", vec![Token::Ident("x".into()), Token::NegSibling, Token::Str("a".into())] },
        greater_than_or_equal = { "d >= 5", vec![Token::Ident("d".into()), Token::Gte, Token::Int(5)] },
        less_than_or_equal = { "d <= 5", vec![Token::Ident("d".into()), Token::Lte, Token::Int(5)] },
    )]
    fn comparison_and_regex_and_ge(input: &str, expected: Vec<Token>) {
        assert2::assert!(toks(input) == expected, "input: {input}");
    }

    #[test]
    fn colon_intrinsic_vs_dot_scope() {
        assert2::assert!(
            toks("span:duration")
                == vec![
                    Token::Ident("span".into()),
                    Token::Colon,
                    Token::Ident("duration".into()),
                ]
        );
        assert2::assert!(
            toks("span.foo")
                == vec![
                    Token::Ident("span".into()),
                    Token::Dot,
                    Token::Ident("foo".into()),
                ]
        );
    }

    #[yare::parameterized(
        nil = { "nil", vec![Token::Nil] },
        booleans = { "true false", vec![Token::Bool(true), Token::Bool(false)] },
        float = { "1.5", vec![Token::Float(1.5)] },
        duration_identifier = { "100ms", vec![Token::Ident("100ms".into())] },
    )]
    fn literals_and_nil_and_durations(input: &str, expected: Vec<Token>) {
        assert2::assert!(toks(input) == expected, "input: {input}");
    }

    #[yare::parameterized(
        start_underscore = { '_', true },
        start_letter = { 'a', true },
        start_digit = { '1', false },
        start_at = { '@', false },
    )]
    fn identifier_start_character_matches_traceql_grammar(ch: char, expected: bool) {
        assert2::assert!(is_ident_start(ch) == expected, "ch: {ch:?}");
    }

    #[yare::parameterized(
        continue_underscore = { '_', true },
        continue_hyphen = { '-', true },
        continue_digit = { '1', true },
        continue_at = { '@', false },
    )]
    fn identifier_continuation_character_matches_traceql_grammar(ch: char, expected: bool) {
        assert2::assert!(is_ident_continue(ch) == expected, "ch: {ch:?}");
    }

    #[yare::parameterized(
        leading_zeroes = { ".05", 0.05 },
        two_digits = { ".99", 0.99 },
        half = { ".5", 0.5 },
        three_decimal_places = { ".009", 0.009 },
    )]
    fn leading_dot_fraction_is_single_float_preserving_zeros(input: &str, expected: f64) {
        assert2::assert!(
            toks(input) == vec![Token::Float(expected)],
            "input: {input}"
        );
    }

    #[test]
    fn leading_dot_ident_remains_dot_scope() {
        // A dot followed by an identifier must stay `Dot` + `Ident`, never a float.
        assert2::assert!(toks(".service") == vec![Token::Dot, Token::Ident("service".into())]);
        assert2::assert!(
            toks(".http.status") == vec![Token::Dot, Token::Ident("http.status".into())]
        );
    }

    #[test]
    fn lone_ampersand_lexes_as_ident_token() {
        // A bare `&` (not part of a union/and operator) lexes to an Ident("&"),
        // not a parse error. Deleting the `'&'` arm in op_token would make the
        // lexer reject it as an unexpected character.
        assert2::assert!(toks("&") == vec![Token::Ident("&".into())]);
        assert2::assert!(lex("&").is_ok());
    }

    #[test]
    fn number_scan_stops_at_a_non_dot_operator_before_a_digit() {
        // `scan_number_or_duration` only folds a `.` into the number when the
        // following char is a digit. The two `&&`s in that guard each matter:
        // weakening either to `||` makes a non-dot operator that precedes a
        // digit (e.g. the `+` in `1+2`) get swallowed into the number, so the
        // whole run is parsed as one float and fails. Asserting `1+2` lexes as
        // three tokens kills both mutants; `1.5` guards the legit-float path.
        assert2::assert!(toks("1+2") == vec![Token::Int(1), Token::Plus, Token::Int(2)]);
        assert2::assert!(toks("1.5") == vec![Token::Float(1.5)]);
    }

    #[yare::parameterized(
        advances = { 0, 1, Some(1) },
        rejects_zero_length = { 1, 0, None },
        rejects_past_end = { 2, 2, None },
    )]
    fn advance_rejects_zero_or_out_of_bounds_progress(
        position: usize,
        length: usize,
        expected: Option<usize>,
    ) {
        assert2::assert!(
            advance("abc", position, length).ok() == expected,
            "pos: {position}, len: {length}"
        );
    }
}

pub(crate) fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch == '-' || ch.is_alphanumeric()
}

pub(crate) fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

pub(crate) fn keyword_or_ident(s: String) -> Token {
    match s.as_str() {
        "nil" => Token::Nil,
        "true" => Token::Bool(true),
        "false" => Token::Bool(false),
        _ => Token::Ident(s),
    }
}

pub(crate) fn no_progress(pos: usize) -> TraceqlError {
    TraceqlError::Parse(format!("lexer made no progress at byte {pos}"))
}

pub(crate) fn advance(input: &str, pos: usize, len: usize) -> Result<usize> {
    if len == 0 {
        return Err(no_progress(pos));
    }
    let next = pos
        .checked_add(len)
        .ok_or_else(|| TraceqlError::Parse(format!("lexer position overflow at byte {pos}")))?;
    if next > input.len() {
        return Err(TraceqlError::Parse(format!(
            "lexer advanced past end of input at byte {pos}"
        )));
    }
    Ok(next)
}

pub(crate) fn scan_ident(s: &str, allow_dots: bool) -> ScanResult<String> {
    let mut end = 0;
    for entry in s.char_indices() {
        let idx = entry.0;
        let ch = entry.1;
        if is_ident_continue(ch) || (allow_dots && ch == '.') {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    ScanResult {
        value: s[..end].to_string(),
        consumed: end,
    }
}

// Lex
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
        let leading_dot_number = ch == '.'
            && rest[1..]
                .chars()
                .next()
                .is_some_and(|next| next.is_ascii_digit());

        if !leading_dot_number && let Some(scanned) = op_token(rest) {
            let next = advance(input, i, scanned.consumed)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = scanned.value == Token::Dot;
            tokens.push(scanned.value);
            continue;
        }

        if ch == '"' {
            let scanned = scan_string(rest)?;
            tokens.push(Token::Str(scanned.value));
            let next = advance(input, i, scanned.consumed)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        if ch.is_ascii_digit() || leading_dot_number {
            let scanned = scan_number_or_duration(rest)?;
            tokens.push(scanned.value);
            let next = advance(input, i, scanned.consumed)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            after_dot = false;
            continue;
        }

        if is_ident_start(ch) {
            let allow_dots = after_dot;
            let scanned = scan_ident(rest, allow_dots);
            tokens.push(keyword_or_ident(scanned.value));
            let next = advance(input, i, scanned.consumed)?;
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

// Op Token
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

// Scan Number Or Duration
pub(crate) fn scan_number_or_duration(s: &str) -> Result<ScanResult<Token>> {
    let mut end = 0;
    let mut has_dot = false;
    let mut chars = s.char_indices().peekable();
    while let Some(entry) = chars.peek().copied() {
        let idx = entry.0;
        let ch = entry.1;
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
            chars.next();
        } else if ch == '.'
            && !has_dot
            && chars
                .clone()
                .nth(1)
                .is_some_and(|entry| entry.1.is_ascii_digit())
        {
            has_dot = true;
            chars.next();
        } else {
            break;
        }
    }

    let mut ident_end = end;
    for entry in s[end..].char_indices() {
        let idx = entry.0;
        let ch = entry.1;
        if is_ident_continue(ch) || ch == 'µ' {
            ident_end = end + idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if ident_end > end {
        return Ok(ScanResult {
            value: Token::Ident(s[..ident_end].to_string()),
            consumed: ident_end,
        });
    }

    if has_dot {
        let v = s[..end]
            .parse::<f64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok(ScanResult {
            value: Token::Float(v),
            consumed: end,
        })
    } else {
        let v = s[..end]
            .parse::<i64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok(ScanResult {
            value: Token::Int(v),
            consumed: end,
        })
    }
}

// Scan String
pub(crate) fn scan_string(s: &str) -> Result<ScanResult<String>> {
    let mut out = String::new();
    let mut escaped = false;
    for entry in s.char_indices().skip(1) {
        let idx = entry.0;
        let ch = entry.1;
        if escaped {
            out.push(match ch {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok(ScanResult {
                value: out,
                consumed: idx + ch.len_utf8(),
            });
        } else {
            out.push(ch);
        }
    }
    Err(TraceqlError::Parse("unterminated string literal".into()))
}

// Token
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    LBrace,
    RBrace,
    LParen,
    RParen,
    Pipe,
    And,
    Or,
    Not,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
    Plus,
    Minus,
    Star,
    Slash,
    Mod,
    Caret,
    Desc,
    Anc,
    Child,
    Parent,
    Sibling,
    NegDesc,
    NegAnc,
    NegChild,
    NegParent,
    NegSibling,
    UnionDesc,
    UnionAnc,
    UnionChild,
    UnionParent,
    UnionSibling,
    Dot,
    Colon,
    Comma,
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
    Eof,
}
