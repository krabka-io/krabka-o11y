use super::*;

pub(crate) fn align_subquery_start(start_ms: i64, step: Time) -> i64 {
    let step_ms = step.millis_i64();
    let remainder = start_ms.rem_euclid(step_ms);
    if remainder == 0 {
        start_ms
    } else {
        start_ms.saturating_add(step_ms - remainder)
    }
}
