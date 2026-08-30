use super::has_word_boundary;

pub(crate) fn split_top_level_set_query(query: &str) -> Option<(&str, &'static str, &str)> {
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
            ch if parens == 0 && brackets == 0 && braces == 0 && ch.is_ascii_alphabetic() => {
                for operator in ["unless", "and", "or"] {
                    if query[index..].starts_with(operator)
                        && has_word_boundary(query, index, operator.len())
                    {
                        return Some((
                            &query[..index],
                            operator,
                            query[index + operator.len()..].trim_start(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    None
}
