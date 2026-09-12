use super::{BTreeMap, Labels, Value};

/// Expands a Prometheus alert template through the shared Go-template runtime.
pub(crate) fn expand_alert_template(tmpl: &str, value: f64, labels: &Labels) -> String {
    expand_alert_template_with_external(tmpl, value, labels, &Labels::new(), "")
}

pub(crate) fn expand_alert_template_with_external(
    tmpl: &str,
    value: f64,
    labels: &Labels,
    external_labels: &Labels,
    external_url: &str,
) -> String {
    let labels = labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect();
    let external_labels = external_labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect();
    let variables = BTreeMap::from([
        (
            "value".into(),
            Value::String(super::format_sample_value(value)),
        ),
        ("labels".into(), Value::Object(labels)),
        ("externalLabels".into(), Value::Object(external_labels)),
        ("externalURL".into(), Value::String(external_url.into())),
    ]);
    krabka_logql::LineFormat::new(tmpl).map_or_else(
        |_| tmpl.to_string(),
        |template| template.render_with_variables("", &BTreeMap::new(), &variables),
    )
}
