use super::{Heatmap, bucket_index, value_bounds};

#[must_use]
pub fn bin_heatmap(
    points: &[crate::TimestampTotalPoint],
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    value_buckets: usize,
) -> Heatmap {
    let bounds = value_bounds(points);
    let min_value = bounds.min;
    let max_value = bounds.max;
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
    for point in points {
        if point.timestamp_ms < start_ms || point.timestamp_ms >= end_ms {
            continue;
        }
        let time_idx = bucket_index(
            i128::from(point.timestamp_ms - start_ms),
            time_span,
            time_buckets,
        );
        let value_idx = if value_span == 0 {
            0
        } else {
            bucket_index(
                i128::from(point.total - min_value),
                value_span,
                value_buckets,
            )
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
