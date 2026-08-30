use super::*;

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
