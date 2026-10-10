/// Splits `query` at the first operator that sits outside every quote,
/// parenthesis, bracket and brace, and returns the left side, the operator,
/// and the right side with its leading whitespace trimmed.
///
/// `operator_at` names the operator that starts at a top-level byte index,
/// if one does; it never sees a quote or a bracket character.
pub(crate) fn split_top_level_query(
    query: &str,
    operator_at: impl Fn(usize, char) -> Option<&'static str>,
) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            _ if parens == 0 && brackets == 0 && braces == 0 => {
                if let Some(operator) = operator_at(index, ch) {
                    return Some((
                        &query[..index],
                        operator,
                        query[index + operator.len()..].trim_start(),
                    ));
                }
            }
            _ => {}
        }
    }
    None
}
