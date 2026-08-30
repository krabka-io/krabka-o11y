use super::{Labels, Value, expand_prometheus_alert_template, json};

pub(crate) fn prometheus_alert_template_map(
    templates: &Labels,
    labels: &Labels,
    value: &str,
) -> Value {
    Value::Object(
        templates
            .iter()
            .map(|(key, template)| {
                (
                    key.clone(),
                    json!(expand_prometheus_alert_template(template, labels, value)),
                )
            })
            .collect(),
    )
}
