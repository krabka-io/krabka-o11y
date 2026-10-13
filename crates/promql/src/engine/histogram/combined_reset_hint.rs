use super::ResetHint;

pub(crate) fn combined_reset_hint(left: ResetHint, right: ResetHint) -> ResetHint {
    if left == right {
        left
    } else if left == ResetHint::Gauge || right == ResetHint::Gauge {
        ResetHint::Gauge
    } else {
        ResetHint::Unknown
    }
}
