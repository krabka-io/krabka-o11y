use super::*;

pub(crate) fn translated_metric_name(
    metric: &Metric,
    strategy: TranslationStrategy,
    add_total: bool,
) -> String {
    let mut name = normalize_name(&metric.name, strategy);
    if add_total && let Some(base) = name.strip_suffix("_total") {
        name = base.to_string();
    }
    if let Some(unit_suffix) = prometheus_unit_suffix(&metric.unit)
        && !name.ends_with(&unit_suffix)
    {
        name.push('_');
        name.push_str(&unit_suffix);
    }
    if add_total && !name.ends_with("_total") {
        name.push_str("_total");
    }
    name
}
