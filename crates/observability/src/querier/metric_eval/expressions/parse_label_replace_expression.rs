use super::*;

pub(crate) fn parse_label_replace_expression(query: &str) -> Option<LabelReplaceExpression> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    let [
        inner_query,
        destination_label,
        replacement,
        source_label,
        pattern,
    ] = arguments.as_slice()
    else {
        return None;
    };

    Some(LabelReplaceExpression {
        query: inner_query.to_string(),
        destination_label: parse_logql_string_argument(destination_label)?,
        replacement: parse_logql_string_argument(replacement)?,
        source_label: parse_logql_string_argument(source_label)?,
        pattern: parse_logql_string_argument(pattern)?,
    })
}
