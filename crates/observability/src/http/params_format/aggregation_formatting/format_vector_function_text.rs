use super::parse_formatted_vector_function;

pub(crate) fn format_vector_function_text(query: &str) -> Option<String> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let (formatted, end) = parse_formatted_vector_function(&query, 0)?;
    (end == query.len()).then_some(formatted)
}
