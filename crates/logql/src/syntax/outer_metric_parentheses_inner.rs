pub(crate) struct SplitMetricParentheses<'a> {
    pub(crate) inner: &'a str,
    pub(crate) remainder: &'a str,
}

pub(crate) fn outer_metric_parentheses_inner(input: &str) -> Option<&str> {
    split_metric_parentheses(input)
        .filter(|split| split.remainder.is_empty())
        .map(|split| split.inner)
}

pub(crate) fn split_metric_parentheses(input: &str) -> Option<SplitMetricParentheses<'_>> {
    if input.chars().next()? != '(' {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter: Option<char> = None;
    let mut escaped = false;
    let mut index = 0;
    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .expect("index is within input");
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if delimiter.eq(&ch) {
                quote_delimiter = None;
            }
            index += ch.len_utf8();
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.checked_sub(1)?;
                if matches!(depth, 0) {
                    let close_end = index.saturating_add(ch.len_utf8());
                    return Some(SplitMetricParentheses {
                        inner: &input[1..index],
                        remainder: &input[close_end..],
                    });
                }
            }
            _ => {}
        }
        index += ch.len_utf8();
    }

    None
}
