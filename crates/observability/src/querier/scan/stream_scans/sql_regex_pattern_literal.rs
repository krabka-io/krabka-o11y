/// The body of the SQL string literal that carries a regex to `regexp_like`.
///
/// `regexp_like` hands its second argument straight to the `regex` crate, so,
/// unlike a `LIKE` pattern, nothing un-escapes it on the way in.
/// [`sql_like_pattern_literal`] doubles backslashes because arrow's `LIKE`
/// treats `\` as its own escape and halves them again; doing that here would
/// turn `\d` into `\\d` and match a literal backslash instead of a digit.
///
/// The quote is the only character that needs escaping. `DataFusion` parses
/// with sqlparser's generic dialect, whose
/// `supports_string_literal_backslash_escape` is `false`, so a backslash
/// inside `'...'` is an ordinary character and `''` is the only escape the
/// lexer honours.
///
/// [`sql_like_pattern_literal`]: super::sql_like_pattern_literal
pub(crate) fn sql_regex_pattern_literal(pattern: &str) -> String {
    pattern.replace('\'', "''")
}
