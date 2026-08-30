use super::{Value, Labels, expand_alert_template, Map};

/// Applies [`expand_alert_template`] to every string value of a JSON object.
///
/// Keys and non-string values stay unchanged. Alert annotation maps use this
/// function.
pub(crate) fn expand_alert_mapping_json(mapping: &Value, value: f64, labels: &Labels) -> Value {
    let Value::Object(object) = mapping else {
        return mapping.clone();
    };
    let expanded = object
        .iter()
        .map(|(key, entry)| {
            let expanded = entry.as_str().map_or_else(
                || entry.clone(),
                |text| Value::String(expand_alert_template(text, value, labels)),
            );
            (key.clone(), expanded)
        })
        .collect::<Map<_, _>>();
    Value::Object(expanded)
}
