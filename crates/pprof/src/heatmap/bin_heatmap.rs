use super::{Heatmap, bucket_index, value_bounds};

#[must_use]
pub fn bin_heatmap(
    points: &[(i64, i64)],
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    value_buckets: usize,
) -> Heatmap {
    let (min_value, max_value) = value_bounds(points);
    let mut counts = vec![vec![0; value_buckets]; time_buckets];
    if start_ms >= end_ms || time_buckets == 0 || value_buckets == 0 {
        return Heatmap {
            start_ms,
            end_ms,
            time_buckets,
            value_buckets,
            min_value,
            max_value,
            counts,
        };
    }

    let time_span = i128::from(end_ms - start_ms);
    let value_span = i128::from(max_value - min_value);
    for (timestamp, value) in points {
        if *timestamp < start_ms || *timestamp >= end_ms {
            continue;
        }
        let time_idx = bucket_index(i128::from(*timestamp - start_ms), time_span, time_buckets);
        let value_idx = if value_span == 0 {
            0
        } else {
            bucket_index(i128::from(*value - min_value), value_span, value_buckets)
        };
        counts[time_idx][value_idx] += 1;
    }

    Heatmap {
        start_ms,
        end_ms,
        time_buckets,
        value_buckets,
        min_value,
        max_value,
        counts,
    }
}
