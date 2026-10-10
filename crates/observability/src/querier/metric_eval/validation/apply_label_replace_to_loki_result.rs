use super::{HttpQueryError, ParseError, Regex, Value, json};

/// The arguments of `label_replace` after its vector operand.
#[derive(Clone, Copy)]
pub(crate) struct LabelReplaceArguments<'a> {
    pub(crate) destination_label: &'a str,
    pub(crate) replacement: &'a str,
    pub(crate) source_label: &'a str,
    pub(crate) pattern: &'a str,
}

/// Applies `label_replace` to every series of `value`. `query` is the full
/// query text that an invalid `pattern` is reported against.
pub(crate) fn apply_label_replace_to_loki_result(
    value: &mut Value,
    arguments: LabelReplaceArguments<'_>,
    query: &str,
) -> Result<(), HttpQueryError> {
    let LabelReplaceArguments {
        destination_label,
        replacement,
        source_label,
        pattern,
    } = arguments;
    let regex = Regex::new(pattern).map_err(|error| HttpQueryError::LokiParse {
        query: query.to_string(),
        source: ParseError::Syntax {
            message: error.to_string(),
            position: 0,
        },
    })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let source_value = metric
            .get(source_label)
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(replacement, &mut destination_value);
            metric.insert(destination_label.to_string(), json!(destination_value));
        }
    }
    Ok(())
}
