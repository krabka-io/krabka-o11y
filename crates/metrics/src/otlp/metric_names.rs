use super::{Metric, TranslationStrategy};

/// Normalizes an OTLP identifier into a Prometheus-compatible metric name or
/// label name.
#[must_use]
pub fn normalize_name(name: &str, strategy: TranslationStrategy) -> String {
    if !strategy.escapes_names() {
        return name.to_string();
    }
    let mut out = String::with_capacity(name.len());
    for entry in name.chars().enumerate() {
        let index = entry.0;
        let ch = entry.1;
        let valid = ch == '_' || ch.is_ascii_alphanumeric();
        if valid && (index != 0 || !ch.is_ascii_digit()) {
            out.push(ch);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    if out.is_empty() { "_".to_string() } else { out }
}

pub(crate) fn translated_metric_name(
    metric: &Metric,
    strategy: TranslationStrategy,
    add_total: bool,
) -> String {
    let mut name = normalize_name(&metric.name, strategy);
    if add_total && let Some(base) = name.strip_suffix("_total") {
        name = base.to_string();
    }
    if strategy.adds_suffixes()
        && let Some(unit_suffix) = prometheus_unit_suffix(&metric.unit)
        && !name.ends_with(&unit_suffix)
    {
        name.push('_');
        name.push_str(&unit_suffix);
    }
    if strategy.adds_suffixes() && add_total && !name.ends_with("_total") {
        name.push_str("_total");
    }
    name
}

pub(crate) fn prometheus_base_unit_suffix(unit: &str) -> Option<&'static str> {
    match unit {
        "d" => Some("days"),
        "h" => Some("hours"),
        "min" => Some("minutes"),
        "s" => Some("seconds"),
        "ms" => Some("milliseconds"),
        "us" => Some("microseconds"),
        "ns" => Some("nanoseconds"),
        "m" => Some("meters"),
        "By" => Some("bytes"),
        "KiBy" => Some("kibibytes"),
        "MiBy" => Some("mebibytes"),
        "GiBy" => Some("gibibytes"),
        "TiBy" => Some("tebibytes"),
        "kBy" => Some("kilobytes"),
        "MBy" => Some("megabytes"),
        "GBy" => Some("gigabytes"),
        "TBy" => Some("terabytes"),
        "bit" => Some("bits"),
        "V" => Some("volts"),
        "A" => Some("amperes"),
        "J" => Some("joules"),
        "W" => Some("watts"),
        "g" => Some("grams"),
        "Cel" => Some("celsius"),
        "Hz" => Some("hertz"),
        "%" => Some("percent"),
        _ => None,
    }
}

pub(crate) fn prometheus_unit_suffix(unit: &str) -> Option<String> {
    let cleaned = strip_ucum_annotations(unit.trim());
    let unit = cleaned.trim();
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if let Some(numerator) = unit.strip_suffix("/s")
        && let Some(numerator) = prometheus_base_unit_suffix(numerator)
    {
        return Some(format!("{numerator}_per_second"));
    }
    prometheus_base_unit_suffix(unit).map(str::to_string)
}

pub(crate) fn strip_ucum_annotations(unit: &str) -> String {
    let mut out = String::with_capacity(unit.len());
    let mut in_annotation = false;
    for ch in unit.chars() {
        match ch {
            '{' => in_annotation = true,
            '}' if in_annotation => in_annotation = false,
            _ if !in_annotation => out.push(ch),
            _ => {}
        }
    }
    out
}
