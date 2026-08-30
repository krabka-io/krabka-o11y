use super::{
    format_logql_quoted_string, format_mixed_metric_vector_expression, parse_logql_string_argument,
    split_logql_function_arguments,
};

pub(crate) fn format_label_replace_metric_vector_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_mixed_metric_vector_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace(\n  {vector},\n  {},\n  {},\n  {},\n  {}\n)",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}
