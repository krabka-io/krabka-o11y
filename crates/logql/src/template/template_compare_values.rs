use super::*;

pub(crate) fn template_compare_values(left: &str, right: &str) -> Option<Ordering> {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) if left.is_finite() && right.is_finite() => left.partial_cmp(&right),
        _ => Some(left.cmp(right)),
    }
}
