#[must_use]
pub fn le_label(le_seconds: f64) -> String {
    if le_seconds.is_infinite() {
        "+Inf".to_string()
    } else {
        le_seconds.to_string()
    }
}
