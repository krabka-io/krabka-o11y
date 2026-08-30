//! Profile heatmap binning.

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn bin_counts_profiles_per_time_value_cell() {
        let points = vec![(0, 0), (10, 5), (60, 30), (90, 35)];

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
        let invalid_range = bin_heatmap(&[(10, 1)], 20, 10, 2, 2);
        assert!(invalid_range.counts == vec![vec![0, 0], vec![0, 0]]);

        let zero_time_buckets = bin_heatmap(&[(10, 1)], 0, 20, 0, 2);
        assert!(zero_time_buckets.counts.is_empty());

        let zero_value_buckets = bin_heatmap(&[(10, 1)], 0, 20, 2, 0);
        assert!(zero_value_buckets.counts == vec![Vec::<u64>::new(), Vec::new()]);
    }

    #[test]
    fn bin_uses_offsets_and_excludes_points_outside_time_range() {
        let points = vec![
            (99, 30),
            (100, 10),
            (149, 20),
            (150, 20),
            (199, 30),
            (200, 10),
        ];

        let heatmap = bin_heatmap(&points, 100, 200, 2, 2);

        assert!(heatmap.min_value == 10 && heatmap.max_value == 30);
        assert!(heatmap.counts == vec![vec![1, 1], vec![0, 2]]);
    }
}

// === split-modules: generated submodules ===
mod bin_heatmap;
mod bucket_index;
mod heatmap_type;
mod labeled_heatmap;
mod value_bounds;

pub use bin_heatmap::bin_heatmap;
use bucket_index::bucket_index;
pub use heatmap_type::Heatmap;
pub use labeled_heatmap::LabeledHeatmap;
use value_bounds::value_bounds;
