use super::*;

pub(crate) fn duration_micros(duration: Option<&Duration>) -> i64 {
    duration.map_or(0, |duration| {
        duration
            .seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(duration.nanos) / 1_000)
    })
}
