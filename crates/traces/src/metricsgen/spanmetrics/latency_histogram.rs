use super::{HistogramBucket, HistogramSummary, NS_PER_SEC};

#[derive(Clone, Debug)]
pub(crate) struct LatencyHistogram {
    pub(crate) bucket_edges_ns: Vec<f64>,
    pub(crate) bucket_counts: Vec<f64>,
    pub(crate) sum_ns: f64,
    pub(crate) count: f64,
}

impl LatencyHistogram {
    pub(crate) fn new(edges_ns: &[f64]) -> Self {
        Self {
            bucket_edges_ns: edges_ns.to_vec(),
            bucket_counts: vec![0.0; edges_ns.len() + 1],
            sum_ns: 0.0,
            count: 0.0,
        }
    }

    pub(crate) fn observe_weighted(&mut self, value_ns: f64, weight: f64) {
        let idx = self
            .bucket_edges_ns
            .iter()
            .position(|&edge| value_ns <= edge)
            .unwrap_or(self.bucket_edges_ns.len());
        self.bucket_counts[idx] += weight;
        self.sum_ns += value_ns * weight;
        self.count += weight;
    }

    pub(crate) fn cumulative_seconds(&self) -> HistogramSummary {
        let mut cumulative = 0.0;
        let buckets = self
            .bucket_edges_ns
            .iter()
            .enumerate()
            .map(|indexed_edge| {
                let i = indexed_edge.0;
                let edge_ns = indexed_edge.1;
                cumulative += self.bucket_counts[i];
                HistogramBucket {
                    upper_bound: *edge_ns / NS_PER_SEC,
                    cumulative_count: cumulative,
                }
            })
            .collect();

        HistogramSummary {
            buckets,
            sum: self.sum_ns / NS_PER_SEC,
            count: self.count,
        }
    }
}
