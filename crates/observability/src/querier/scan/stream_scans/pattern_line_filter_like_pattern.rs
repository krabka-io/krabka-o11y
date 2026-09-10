use super::sql_like_pattern_literal;

/// The `LIKE` pattern equivalent to a `LogQL` `|>` pattern filter, if one exists.
///
/// A pattern filter matches when the pattern's literal runs occur in the line
/// in order, with anything at all between them -- `<_>` is a placeholder, and
/// the ends are not anchored. `LIKE '%a%b%'` says exactly that: arrow lowers
/// it to the unanchored, dot-all regex `a.*b`, so the two agree line for line,
/// including on lines that span newlines.
///
/// `None` when the pattern has no literal run to match on. `<_>` alone matches
/// every line and the empty pattern matches none, and neither is worth a
/// predicate: the first prunes nothing, and the second is rare enough that the
/// row-by-row pass can keep it. Returning `None` for both also keeps the
/// negated form honest -- `NOT LIKE '%%'` would reject every row, while
/// `!> ""` in fact rejects none.
pub(crate) fn pattern_line_filter_like_pattern(pattern: &str) -> Option<String> {
    let literals = pattern
        .split("<_>")
        .filter(|literal| !literal.is_empty())
        .collect::<Vec<_>>();
    if literals.is_empty() {
        return None;
    }

    let mut like = String::from("%");
    for literal in literals {
        like.push_str(&sql_like_pattern_literal(literal));
        like.push('%');
    }
    Some(like)
}
