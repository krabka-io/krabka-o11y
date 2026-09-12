use super::*;

pub(crate) fn cumulative_buckets_seconds(
    bucket_edges_ns: &[f64],
    bucket_counts: &[f64],
) -> Vec<(f64, f64)> {
    let mut cumulative = 0.0;
    bucket_edges_ns
        .iter()
        .enumerate()
        .map(|(idx, edge_ns)| {
            cumulative += bucket_counts.get(idx).copied().unwrap_or_default();
            (*edge_ns / NS_PER_SEC, cumulative)
        })
        .collect()
}
