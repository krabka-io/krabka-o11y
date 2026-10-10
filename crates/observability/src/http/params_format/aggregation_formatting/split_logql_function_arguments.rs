use crate::http::params_format::QuotedTextScanner;

pub(crate) fn split_logql_function_arguments<'a>(
    query: &'a str,
    name: &str,
) -> Option<Vec<&'a str>> {
    let query = query.trim();
    let rest = query.strip_prefix(name)?.trim_start();
    let rest = rest.strip_prefix('(')?;
    let mut arguments = Vec::new();
    let mut start = 0;
    let mut parens = 0_i32;
    let mut quotes = QuotedTextScanner::default();
    for (index, ch) in rest.char_indices() {
        if quotes.in_quotes(ch) {
            continue;
        }
        match ch {
            '(' => parens += 1,
            ')' if parens > 0 => parens -= 1,
            ',' if parens == 0 => {
                arguments.push(rest[start..index].trim());
                start = index + ch.len_utf8();
            }
            ')' => {
                arguments.push(rest[start..index].trim());
                if rest[index + ch.len_utf8()..].trim().is_empty() {
                    return Some(arguments);
                }
                return None;
            }
            _ => {}
        }
    }
    None
}
