use super::{
    format_label_replace_arguments, format_vector_only_expression, split_logql_function_arguments,
};

pub(crate) fn format_vector_label_replace_function(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_vector_only_expression(arguments[0].trim())?;
    format_label_replace_arguments(&vector, &arguments[1..])
}
