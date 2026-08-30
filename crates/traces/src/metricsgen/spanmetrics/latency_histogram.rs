use super::*;

#[derive(Clone, Debug)]
pub(crate) struct LatencyHistogram {
    pub(crate) bucket_edges_ns: Vec<f64>,
    pub(crate) bucket_counts: Vec<u64>,
    pub(crate) sum_ns: f64,
    pub(crate) count: u64,
}

impl LatencyHistogram {
    pub(crate) fn new(edges_ns: &[f64]) -> Self {
        Self {
            bucket_edges_ns: edges_ns.to_vec(),
            bucket_counts: vec![0; edges_ns.len() + 1],
            sum_ns: 0.0,
            count: 0,
        }
    }

    pub(crate) fn observe(&mut self, value_ns: f64) {
        let idx = self
            .bucket_edges_ns
            .iter()
            .position(|&edge| value_ns <= edge)
            .unwrap_or(self.bucket_edges_ns.len());
        self.bucket_counts[idx] += 1;
        self.sum_ns += value_ns;
        self.count += 1;
    }

    pub(crate) fn cumulative_seconds(&self) -> (Vec<(f64, f64)>, f64, f64) {
        let mut cumulative = 0_u64;
        let buckets = self
            .bucket_edges_ns
            .iter()
            .enumerate()
            .map(|(i, edge_ns)| {
                cumulative += self.bucket_counts[i];
                (
                    *edge_ns / NS_PER_SEC,
                    cumulative.to_f64().unwrap_or(f64::MAX),
                )
            })
            .collect();

        (
            buckets,
            self.sum_ns / NS_PER_SEC,
            self.count.to_f64().unwrap_or(f64::MAX),
        )
    }
}
