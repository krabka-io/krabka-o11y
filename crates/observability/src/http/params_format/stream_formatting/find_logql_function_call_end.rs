pub(crate) fn find_logql_function_call_end(
    query: &str,
    position: usize,
    name: &str,
) -> Option<usize> {
    let rest = &query[position..];
    let rest = rest.strip_prefix(name)?;
    let mut chars = rest.char_indices();
    let (_, first) = chars.next()?;
    if first != '(' {
        return None;
    }

    let mut parens = 1_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in chars {
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
            ')' => {
                parens -= 1;
                if parens == 0 {
                    return Some(position + name.len() + index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}
