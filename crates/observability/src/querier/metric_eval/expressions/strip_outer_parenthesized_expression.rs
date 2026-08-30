use super::*;

pub(crate) fn strip_outer_parenthesized_expression(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter = None;
    let mut escaped = false;
    for (index, ch) in trimmed.char_indices() {
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote_delimiter = None;
            }
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && index != trimmed.len() - ch.len_utf8() {
                    return None;
                }
            }
            _ => {}
        }
    }

    if depth == 0 {
        Some(trimmed[1..trimmed.len() - 1].trim())
    } else {
        None
    }
}
