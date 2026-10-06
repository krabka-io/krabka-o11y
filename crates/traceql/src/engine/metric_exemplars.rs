use super::{MetricBucket, TraceMetricExemplar};

pub(crate) struct ExemplarBuckets {
    counts: Vec<usize>,
    total: usize,
    limit: usize,
    start_ms: i64,
    end_ms: i64,
    width_ms: i64,
}

impl ExemplarBuckets {
    pub(crate) fn new(limit: usize, start_ns: i64, end_ns: i64) -> Self {
        let count = (limit / 2).max(1);
        let lower_bound = start_ns.div_euclid(1_000_000);
        let upper_bound = end_ns.div_euclid(1_000_000);
        let width_ms =
            ((upper_bound - lower_bound) / i64::try_from(count).unwrap_or(i64::MAX)).max(1);
        Self {
            counts: vec![0; count],
            total: 0,
            limit,
            start_ms: lower_bound,
            end_ms: upper_bound,
            width_ms,
        }
    }

    pub(crate) fn admit(&mut self, timestamp_ns: i64) -> bool {
        let timestamp = timestamp_ns.div_euclid(1_000_000);
        if self.total >= self.limit || timestamp < self.start_ms || timestamp > self.end_ms {
            return false;
        }
        let last = self.counts.len() - 1;
        let bucket = usize::try_from((timestamp - self.start_ms) / self.width_ms)
            .unwrap_or(last)
            .min(last);
        if self.counts[bucket] >= 2 {
            return false;
        }
        self.counts[bucket] += 1;
        self.total += 1;
        true
    }
}

pub(crate) fn metric_exemplars(
    buckets: &[MetricBucket],
    max_exemplars: usize,
    start_ns: i64,
    end_ns: i64,
) -> Vec<TraceMetricExemplar> {
    let mut admission = ExemplarBuckets::new(max_exemplars, start_ns, end_ns);
    buckets
        .iter()
        .flat_map(|bucket| &bucket.exemplars)
        .filter(|exemplar| admission.admit(exemplar.timestamp_ns))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn timestamps_share_two_exemplar_slots_per_stratum_and_end_is_inclusive() {
        let mut buckets = ExemplarBuckets::new(6, 1_000_000_000, 7_000_000_000);
        let decisions = [
            999_000_000,
            1_000_000_000,
            2_999_000_000,
            2_500_000_000,
            3_000_000_000,
            4_999_000_000,
            4_500_000_000,
            5_000_000_000,
            7_000_000_000,
            7_000_000_001,
        ]
        .map(|timestamp| buckets.admit(timestamp));
        assert!(
            decisions
                == [
                    false, true, true, false, true, true, false, true, true, false
                ]
        );
        assert!(!buckets.admit(6_000_000_000));
        let mut few = ExemplarBuckets::new(1, 0, 1_000_000);
        assert!(few.admit(0));
        assert!(!few.admit(1_000_000));
        let mut none = ExemplarBuckets::new(0, 0, 0);
        assert!(!none.admit(0));
    }
}
