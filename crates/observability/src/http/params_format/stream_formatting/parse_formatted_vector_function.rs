use super::{
    find_logql_function_call_end, format_vector_label_replace_function, parse_scalar_sample,
};

pub(crate) fn parse_formatted_vector_function(
    query: &str,
    position: usize,
) -> Option<(String, usize)> {
    if let Some(scalar) = query[position..].strip_prefix("vector(") {
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        let sample = parse_scalar_sample(scalar_text)?.format_fixed_six();
        return Some((
            format!("vector({sample})"),
            position + "vector(".len() + scalar_end + 1,
        ));
    }

    let call_end = find_logql_function_call_end(query, position, "label_replace")?;
    let formatted = format_vector_label_replace_function(&query[position..call_end])?;
    Some((formatted, call_end))
}
