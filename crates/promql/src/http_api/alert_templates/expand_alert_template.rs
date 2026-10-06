use super::{BTreeMap, Labels};

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
    let map = BTreeMap::from([("expanded".to_owned(), tmpl.to_owned())]);
    crate::ruler::expand_alert_label_map(
        &map,
        value,
        &labels.clone().into(),
        external_labels,
        external_url,
        &BTreeMap::new(),
    )
    .remove("expanded")
    .expect("one expansion requested")
    .as_str()
    .to_owned()
}
