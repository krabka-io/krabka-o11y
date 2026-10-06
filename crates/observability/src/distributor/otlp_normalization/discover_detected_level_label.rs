use krabka_logql::LogfmtParser;

use super::{Labels, Limits};

mod detect_entry_level;
mod level_fields;
mod normalize_level;

use detect_entry_level::detect_entry_level;
use level_fields::level_fields;
use normalize_level::normalize_level;

// Loki 3.7.7 distributor/field_detection.go adds discovery to entry metadata,
// after validating user metadata. The immutable stream labels never change.
pub(crate) fn discover_detected_level_label(
    labels: &Labels,
    metadata: &mut Labels,
    line: &str,
    limits: &Limits,
) {
    if !limits.discover_log_levels {
        return;
    }
    if let Some(existing) = metadata.get_mut("detected_level") {
        *existing = normalize_level(existing).to_string();
        return;
    }
    let value = level_fields(limits)
        .find_map(|name| labels.get(name))
        .or_else(|| level_fields(limits).find_map(|name| metadata.get(name)))
        .map_or_else(
            || detect_entry_level(line, metadata, limits),
            |value| normalize_level(value).to_string(),
        );
    if !value.is_empty() {
        metadata.insert("detected_level".to_string(), value);
    }
}

#[cfg(test)]
mod tests;
