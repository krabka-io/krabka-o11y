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

mod advance;
mod is_ident_continue;
mod is_ident_start;
mod keyword_or_ident;
mod lex;
mod no_progress;
mod op_token;
mod scan_ident;
mod scan_number_or_duration;
mod scan_string;
mod token;

use advance::advance;
use is_ident_continue::is_ident_continue;
use is_ident_start::is_ident_start;
use keyword_or_ident::keyword_or_ident;
pub use lex::lex;
use no_progress::no_progress;
use op_token::op_token;
use scan_ident::scan_ident;
use scan_number_or_duration::scan_number_or_duration;
use scan_string::scan_string;
pub use token::Token;
