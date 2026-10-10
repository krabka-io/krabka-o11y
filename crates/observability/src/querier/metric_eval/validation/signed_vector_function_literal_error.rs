use super::{could_be_scalar_vector_expression, first_unquoted_match};

pub(crate) fn signed_vector_function_literal_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    first_unquoted_match(query, |index, _| {
        if !query[index..].starts_with("vector(") {
            return None;
        }
        let mut sign_index = index + "vector(".len();
        while let Some(next) = query[sign_index..].chars().next() {
            if !next.is_whitespace() {
                break;
            }
            sign_index += next.len_utf8();
        }
        let sign @ ('+' | '-') = query[sign_index..].chars().next()? else {
            return None;
        };
        let column = query[..sign_index].chars().count() + 1;
        Some(format!(
            "parse error at line 1, col {column}: syntax error: unexpected {sign}, expecting NUMBER"
        ))
    })
}
