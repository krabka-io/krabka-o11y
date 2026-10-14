use super::{super::HistogramBucket, NS_PER_SEC};

pub(crate) fn cumulative_buckets_seconds(
    bucket_edges_ns: &[f64],
    bucket_counts: &[f64],
) -> Vec<HistogramBucket> {
    let mut cumulative = 0.0;
    bucket_edges_ns
        .iter()
        .enumerate()
        .map(|indexed_edge| {
            let idx = indexed_edge.0;
            let edge_ns = indexed_edge.1;
            cumulative += bucket_counts.get(idx).copied().unwrap_or_default();
            HistogramBucket {
                upper_bound: *edge_ns / NS_PER_SEC,
                cumulative_count: cumulative,
            }
        })
        .collect()
}
