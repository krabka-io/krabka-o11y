use super::*;

pub(crate) fn apply_label_replace_to_loki_result(
    value: &mut Value,
    destination_label: &str,
    replacement: &str,
    source_label: &str,
    pattern: &str,
    query: &str,
) -> Result<(), HttpQueryError> {
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
