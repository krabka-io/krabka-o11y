use super::TranslationStrategy;

/// Normalizes an OTLP identifier into a Prometheus-compatible metric name or
/// label name.
#[must_use]
pub fn normalize_name(name: &str, _strategy: TranslationStrategy) -> String {
    let mut out = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        let valid = ch == '_' || ch.is_ascii_alphanumeric();
        if valid && (index != 0 || !ch.is_ascii_digit()) {
            out.push(ch);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    if out.is_empty() { "_".to_string() } else { out }
}
