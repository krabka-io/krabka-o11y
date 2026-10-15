//! Profile heatmap binning.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heatmap {
    pub start_ms: i64,
    pub end_ms: i64,
    pub time_buckets: usize,
    pub value_buckets: usize,
    pub min_value: i64,
    pub max_value: i64,
    pub counts: Vec<Vec<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmap {
    pub labels: Vec<crate::ProfileLabel>,
    pub heatmap: Heatmap,
}

/// Label group and unbinned profile totals `(timestamp_ms, value)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmapPoints {
    pub labels: Vec<crate::ProfileLabel>,
    pub points: Vec<crate::TimestampTotalPoint>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn bin_counts_profiles_per_time_value_cell() {
        let points = vec![
            crate::TimestampTotalPoint {
                timestamp_ms: 0,
                total: 0,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 10,
                total: 5,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 60,
                total: 30,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 90,
                total: 35,
            },
        ];

        let heatmap = bin_heatmap(&points, 0, 100, 2, 2);

        assert!(
            heatmap
                == Heatmap {
                    start_ms: 0,
                    end_ms: 100,
                    time_buckets: 2,
                    value_buckets: 2,
                    min_value: 0,
                    max_value: 35,
                    counts: vec![vec![2, 0], vec![0, 2]],
                }
        );
    }

    #[test]
    fn bin_returns_empty_counts_for_invalid_ranges_or_zero_buckets() {
        let invalid_range = bin_heatmap(
            &[crate::TimestampTotalPoint {
                timestamp_ms: 10,
                total: 1,
            }],
            20,
            10,
            2,
            2,
        );
        assert!(invalid_range.counts == vec![vec![0, 0], vec![0, 0]]);

        let zero_time_buckets = bin_heatmap(
            &[crate::TimestampTotalPoint {
                timestamp_ms: 10,
                total: 1,
            }],
            0,
            20,
            0,
            2,
        );
        assert!(zero_time_buckets.counts.is_empty());

        let zero_value_buckets = bin_heatmap(
            &[crate::TimestampTotalPoint {
                timestamp_ms: 10,
                total: 1,
            }],
            0,
            20,
            2,
            0,
        );
        assert!(zero_value_buckets.counts == vec![Vec::<u64>::new(), Vec::new()]);
    }

    #[test]
    fn bin_uses_offsets_and_excludes_points_outside_time_range() {
        let points = vec![
            crate::TimestampTotalPoint {
                timestamp_ms: 99,
                total: 30,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 100,
                total: 10,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 149,
                total: 20,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 150,
                total: 20,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 199,
                total: 30,
            },
            crate::TimestampTotalPoint {
                timestamp_ms: 200,
                total: 10,
            },
        ];

        let heatmap = bin_heatmap(&points, 100, 200, 2, 2);

        assert!(heatmap.min_value == 10 && heatmap.max_value == 30);
        assert!(heatmap.counts == vec![vec![1, 1], vec![0, 2]]);
    }
}

mod bin_heatmap;
mod value_bounds;

pub use bin_heatmap::bin_heatmap;
use value_bounds::value_bounds;

pub(crate) fn bucket_index(offset: i128, span: i128, buckets: usize) -> usize {
    let raw = offset * i128::try_from(buckets).expect("bucket count fits i128") / span;
    let clamped = raw.clamp(
        0,
        i128::try_from(buckets - 1).expect("bucket count fits i128"),
    );
    usize::try_from(clamped).expect("bucket index fits usize")
}
