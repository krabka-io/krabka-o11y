use super::*;

pub(crate) fn format_vector_label_replace_function(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_vector_only_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace({vector},{},{},{},{})",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}
