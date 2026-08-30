//! `TraceQL` lexer.

use crate::error::{Result, TraceqlError};

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn toks(s: &str) -> Vec<Token> {
        let mut t = lex(s).unwrap();
        assert!(t.pop() == Some(Token::Eof));
        t
    }

    /// A decimal point is only part of a number when a digit follows it, and
    /// only the first one is. Everything else is a separate token, so the
    /// scanner has to stop in the right place rather than swallow the rest.
    #[test]
    fn a_decimal_point_belongs_to_a_number_only_when_a_digit_follows() {
        assert!(toks("1") == vec![Token::Int(1)]);
        assert!(toks("1.5") == vec![Token::Float(1.5)]);
        assert!(toks("0.25") == vec![Token::Float(0.25)]);
        assert!(
            toks("10.75") == vec![Token::Float(10.75)],
            "more than one digit each side"
        );

        // A second point is not part of the number, so it ends there.
        assert!(
            toks("1.5.5") == vec![Token::Float(1.5), Token::Float(0.5)],
            "the first point wins and the rest lexes on its own"
        );

        // A point with no digit after it is not part of the number at all.
        assert!(toks("1 .5") == vec![Token::Int(1), Token::Float(0.5)]);
        assert!(
            toks("1.") == vec![Token::Int(1), Token::Dot],
            "a trailing point ends the number rather than joining it"
        );
    }

    #[test]
    fn single_equals_no_double() {
        assert!(
            toks(".http.status = 200")
                == vec![
                    Token::Dot,
                    Token::Ident("http.status".into()),
                    Token::Eq,
                    Token::Int(200),
                ]
        );
        assert!(lex("a == b").is_err());
    }

    #[test]
    fn structural_maximal_munch() {
        for (input, mid) in [
            ("a >> b", Token::Desc),
            ("a !>> b", Token::NegDesc),
            ("a &>> b", Token::UnionDesc),
            ("a !> b", Token::NegChild),
            ("a &~ b", Token::UnionSibling),
        ] {
            assert!(
                toks(input) == vec![Token::Ident("a".into()), mid, Token::Ident("b".into())],
                "input: {input}"
            );
        }
    }

    #[test]
    fn comparison_and_regex_and_ge() {
        for (input, want) in [
            (
                "x =~ \"a.*\"",
                vec![
                    Token::Ident("x".into()),
                    Token::Re,
                    Token::Str("a.*".into()),
                ],
            ),
            (
                "x !~ \"a\"",
                vec![Token::Ident("x".into()), Token::Nre, Token::Str("a".into())],
            ),
            (
                "d >= 5",
                vec![Token::Ident("d".into()), Token::Gte, Token::Int(5)],
            ),
            (
                "d <= 5",
                vec![Token::Ident("d".into()), Token::Lte, Token::Int(5)],
            ),
        ] {
            assert!(toks(input) == want, "input: {input}");
        }
    }

    #[test]
    fn colon_intrinsic_vs_dot_scope() {
        assert!(
            toks("span:duration")
                == vec![
                    Token::Ident("span".into()),
                    Token::Colon,
                    Token::Ident("duration".into()),
                ]
        );
        assert!(
            toks("span.foo")
                == vec![
                    Token::Ident("span".into()),
                    Token::Dot,
                    Token::Ident("foo".into()),
                ]
        );
    }

    #[test]
    fn literals_and_nil_and_durations() {
        for (input, want) in [
            ("nil", vec![Token::Nil]),
            ("true false", vec![Token::Bool(true), Token::Bool(false)]),
            ("1.5", vec![Token::Float(1.5)]),
            ("100ms", vec![Token::Ident("100ms".into())]),
        ] {
            assert!(toks(input) == want, "input: {input}");
        }
    }

    #[test]
    fn identifier_character_helpers_match_traceql_grammar() {
        for (ch, want) in [('_', true), ('a', true), ('1', false), ('@', false)] {
            assert!(is_ident_start(ch) == want, "ch: {ch:?}");
        }

        for (ch, want) in [('_', true), ('-', true), ('1', true), ('@', false)] {
            assert!(is_ident_continue(ch) == want, "ch: {ch:?}");
        }
    }

    #[test]
    fn leading_dot_fraction_is_single_float_preserving_zeros() {
        for (input, want) in [(".05", 0.05), (".99", 0.99), (".5", 0.5), (".009", 0.009)] {
            assert!(toks(input) == vec![Token::Float(want)], "input: {input}");
        }
    }

    #[test]
    fn leading_dot_ident_remains_dot_scope() {
        // A dot followed by an identifier must stay `Dot` + `Ident`, never a float.
        assert!(toks(".service") == vec![Token::Dot, Token::Ident("service".into())]);
        assert!(toks(".http.status") == vec![Token::Dot, Token::Ident("http.status".into())]);
    }

    #[test]
    fn lone_ampersand_lexes_as_ident_token() {
        // A bare `&` (not part of a union/and operator) lexes to an Ident("&"),
        // not a parse error. Deleting the `'&'` arm in op_token would make the
        // lexer reject it as an unexpected character.
        assert!(toks("&") == vec![Token::Ident("&".into())]);
        assert!(lex("&").is_ok());
    }

    #[test]
    fn number_scan_stops_at_a_non_dot_operator_before_a_digit() {
        // `scan_number_or_duration` only folds a `.` into the number when the
        // following char is a digit. The two `&&`s in that guard each matter:
        // weakening either to `||` makes a non-dot operator that precedes a
        // digit (e.g. the `+` in `1+2`) get swallowed into the number, so the
        // whole run is parsed as one float and fails. Asserting `1+2` lexes as
        // three tokens kills both mutants; `1.5` guards the legit-float path.
        assert!(toks("1+2") == vec![Token::Int(1), Token::Plus, Token::Int(2)]);
        assert!(toks("1.5") == vec![Token::Float(1.5)]);
    }

    #[test]
    fn advance_rejects_zero_or_out_of_bounds_progress() {
        for (pos, len, want) in [(0, 1, Some(1)), (1, 0, None), (2, 2, None)] {
            assert!(
                advance("abc", pos, len).ok() == want,
                "pos: {pos}, len: {len}"
            );
        }
    }
}

// === split-modules: generated submodules ===
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
