use super::*;

pub(crate) fn scalar_vector_plain_parse_error(query: &str) -> Option<String> {
    signed_vector_function_literal_error(query)
        .or_else(|| unspaced_vector_set_operator_error(query))
}
