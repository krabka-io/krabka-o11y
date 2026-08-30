
pub(crate) fn ns_floor_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000)
}
