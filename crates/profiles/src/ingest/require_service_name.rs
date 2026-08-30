use super::Labels;

/// Inject `service_name="unknown_service"` when absent or empty.
pub fn require_service_name(labels: &mut Labels) {
    if labels.get("service_name").unwrap_or("").is_empty() {
        labels.insert("service_name", "unknown_service");
    }
}
