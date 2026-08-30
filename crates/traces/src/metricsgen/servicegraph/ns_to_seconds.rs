use super::*;

pub(crate) fn ns_to_seconds(ns: i64) -> f64 {
    ns.max(0).to_f64().unwrap_or(f64::MAX) / NS_PER_SEC
}
