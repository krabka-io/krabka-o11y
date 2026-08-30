use super::*;

pub(crate) fn discover_detected_level_label(labels: &mut Labels, line: &str) {
    if labels.contains_key("detected_level")
        || labels.contains_key("level")
        || labels.contains_key("severity")
        || labels.contains_key("severity_text")
    {
        return;
    }

    let level = detect_log_level(line);
    if let Some(level) = level {
        labels.insert("detected_level".to_string(), level.to_string());
    }
}
