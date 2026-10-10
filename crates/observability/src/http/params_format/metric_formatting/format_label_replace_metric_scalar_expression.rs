use super::{
    format_label_replace_arguments, format_metric_scalar_vector_expression,
    split_logql_function_arguments,
};

pub(crate) fn format_label_replace_metric_scalar_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_metric_scalar_vector_expression(arguments[0].trim())?;
    format_label_replace_arguments(&vector, &arguments[1..])
}
