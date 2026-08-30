use super::*;

pub(crate) fn observe_latency(bucket_edges_ns: &[f64], bucket_counts: &mut [u64], ns: i64) {
    let value_ns = ns.max(0).to_f64().unwrap_or(f64::MAX);
    let idx = bucket_edges_ns
        .iter()
        .position(|edge| value_ns <= *edge)
        .unwrap_or(bucket_edges_ns.len());
    if let Some(count) = bucket_counts.get_mut(idx) {
        *count += 1;
    }
}
