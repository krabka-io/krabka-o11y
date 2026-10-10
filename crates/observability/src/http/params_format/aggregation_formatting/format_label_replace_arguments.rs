use super::{format_logql_quoted_string, parse_logql_string_argument};

/// Prints `label_replace` with an already formatted vector operand and the
/// four string arguments that follow it, or `None` when one of them is not a
/// `LogQL` string.
pub(crate) fn format_label_replace_arguments(
    vector: &str,
    string_arguments: &[&str],
) -> Option<String> {
    let mut formatted = format!("label_replace({vector}");
    for argument in string_arguments {
        let argument = parse_logql_string_argument(argument.trim())?;
        formatted.push(',');
        formatted.push_str(&format_logql_quoted_string(&argument));
    }
    formatted.push(')');
    Some(formatted)
}
