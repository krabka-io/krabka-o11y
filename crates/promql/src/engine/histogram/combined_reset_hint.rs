use super::*;

pub(crate) fn combined_reset_hint(left: ResetHint, right: ResetHint) -> ResetHint {
    match (left, right) {
        (left, right) if left == right => left,
        (ResetHint::Gauge, _) | (_, ResetHint::Gauge) => ResetHint::Gauge,
        _ => ResetHint::Unknown,
    }
}
