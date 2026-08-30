pub(crate) fn split_top_level_arithmetic_query(query: &str) -> Option<(&str, &'static str, &str)> {
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
            '+' | '-' | '*' | '/' | '%' | '^' if parens == 0 && brackets == 0 && braces == 0 => {
                let right = query[index + ch.len_utf8()..].trim_start();
                return Some((
                    &query[..index],
                    match ch {
                        '+' => "+",
                        '-' => "-",
                        '*' => "*",
                        '/' => "/",
                        '%' => "%",
                        '^' => "^",
                        _ => unreachable!(),
                    },
                    right,
                ));
            }
            _ => {}
        }
    }
    None
}
