use super::*;

pub(crate) fn ns_ceil_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000) + i64::from(ns.rem_euclid(1_000_000_000) != 0)
}
