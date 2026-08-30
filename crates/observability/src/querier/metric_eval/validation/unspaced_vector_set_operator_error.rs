use super::*;

pub(crate) fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
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
        if ch == ')' {
            let next_index = index + ch.len_utf8();
            if ["and", "or", "unless"]
                .iter()
                .any(|operator| query[next_index..].starts_with(operator))
            {
                let column = query[..next_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected IDENTIFIER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}
