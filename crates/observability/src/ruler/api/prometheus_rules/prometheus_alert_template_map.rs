use super::{Labels, Value, expand_prometheus_alert_template, json};

pub(crate) fn prometheus_alert_template_map(
    templates: &Labels,
    labels: &Labels,
    value: &str,
) -> Value {
    let mut rendered = serde_json::Map::new();
    for entry in templates {
        rendered.insert(
            entry.0.clone(),
            json!(expand_prometheus_alert_template(entry.1, labels, value)),
        );
    }
    Value::Object(rendered)
}
