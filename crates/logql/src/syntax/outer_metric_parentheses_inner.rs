pub(crate) fn outer_metric_parentheses_inner(input: &str) -> Option<&str> {
    split_metric_parentheses(input)
        .filter(|(_, rest)| rest.is_empty())
        .map(|(inner, _)| inner)
}

pub(crate) fn split_metric_parentheses(input: &str) -> Option<(&str, &str)> {
    let mut chars = input.char_indices();
    if chars.next()?.1 != '(' {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter: Option<char> = None;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if delimiter.eq(&ch) {
                quote_delimiter = None;
            }
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.checked_sub(1)?;
                if matches!(depth, 0) {
                    let close_end = index.saturating_add(ch.len_utf8());
                    return Some((&input[1..index], &input[close_end..]));
                }
            }
            _ => {}
        }
    }

    None
}
