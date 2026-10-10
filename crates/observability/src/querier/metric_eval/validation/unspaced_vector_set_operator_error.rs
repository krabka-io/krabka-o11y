use super::{could_be_scalar_vector_expression, first_unquoted_match};

pub(crate) fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    first_unquoted_match(query, |index, ch| {
        if ch != ')' {
            return None;
        }
        let next_index = index + ch.len_utf8();
        if !["and", "or", "unless"]
            .iter()
            .any(|operator| query[next_index..].starts_with(operator))
        {
            return None;
        }
        let column = query[..next_index].chars().count() + 1;
        Some(format!(
            "parse error at line 1, col {column}: syntax error: unexpected IDENTIFIER"
        ))
    })
}
