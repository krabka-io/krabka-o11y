use super::*;

pub(crate) fn signed_vector_function_literal_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is always on a char boundary");
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += ch.len_utf8();
            continue;
        }
        if query[index..].starts_with("vector(") {
            let mut sign_index = index + "vector(".len();
            while let Some(next) = query[sign_index..].chars().next() {
                if !next.is_whitespace() {
                    break;
                }
                sign_index += next.len_utf8();
            }
            if let Some(sign @ ('+' | '-')) = query[sign_index..].chars().next() {
                let column = query[..sign_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected {sign}, expecting NUMBER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}
