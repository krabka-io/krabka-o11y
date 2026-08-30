use super::*;

pub(crate) fn format_sort_vector_expression(query: &str) -> Option<String> {
    for function in ["sort", "sort_desc"] {
        let Some(arguments) = split_logql_function_arguments(query, function) else {
            continue;
        };
        if arguments.len() != 1 {
            return None;
        }
        let inner = format_loki_vector_expression(arguments[0].trim())?;
        if inner.contains('\n') {
            return Some(format!(
                "{function}(\n{}\n)",
                indent_logql_lines(&inner, "  ")
            ));
        }
        return Some(format!("{function}({inner})"));
    }
    None
}
