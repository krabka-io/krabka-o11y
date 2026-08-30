use super::*;

pub(crate) fn ms_to_seconds(ms: i64) -> f64 {
    ms.to_f64()
        .expect("every i64 has a finite f64 representation")
        / 1000.0
}
