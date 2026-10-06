use num_traits::ToPrimitive;

use super::{BTreeMap, label_pairs, pb};

// Pinned Pyroscope RangeHeatmap uses shared, integer Y boundaries and sparse
// right-closed time slots anchored to the original query start.
pub(crate) fn heatmap_from_points(
    groups: Vec<krabka_pprof::LabeledHeatmapPoints>,
    start: i64,
    end: i64,
    step: i64,
    buckets: usize,
) -> Vec<pb::querier::v1::HeatmapSeries> {
    if buckets == 0 || step <= 0 || start >= end {
        return Vec::new();
    }
    let mut values = groups
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(_, value)| *value));
    let Some(first) = values.next() else {
        return Vec::new();
    };
    let (min, max) = values.fold((first, first.max(0)), |(min, max), value| {
        (min.min(value), max.max(value))
    });
    let max = if min == max {
        max.saturating_add(1)
    } else {
        max
    };
    let width = (i128::from(max) - i128::from(min))
        .to_f64()
        .unwrap_or(f64::MAX)
        / buckets.to_f64().unwrap_or(f64::MAX);
    let boundaries: Vec<i64> = (0..buckets)
        .map(|index| {
            min.saturating_add(
                (index.to_f64().unwrap_or(f64::MAX) * width)
                    .to_i64()
                    .unwrap_or(i64::MAX),
            )
        })
        .collect();
    groups
        .into_iter()
        .filter_map(|(labels, points)| {
            let mut slots: BTreeMap<i64, pb::querier::v1::HeatmapSlot> = BTreeMap::new();
            for (timestamp, value) in points {
                if timestamp < start.saturating_sub(step) || timestamp > end {
                    continue;
                }
                let Some(timestamp) = super::heatmap_slot_timestamp(start, end, step, timestamp)
                else {
                    continue;
                };
                let index = boundaries
                    .windows(2)
                    .position(|pair| value >= pair[0] && value < pair[1])
                    .unwrap_or(buckets - 1);
                let slot = slots
                    .entry(timestamp)
                    .or_insert_with(|| pb::querier::v1::HeatmapSlot {
                        timestamp,
                        y_min: boundaries
                            .iter()
                            .map(|value| value.to_f64().unwrap_or(f64::MAX))
                            .collect(),
                        counts: vec![0; buckets],
                        exemplars: Vec::new(),
                    });
                slot.counts[index] += 1;
            }
            (!slots.is_empty()).then(|| pb::querier::v1::HeatmapSeries {
                labels: label_pairs(labels),
                slots: slots.into_values().collect(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn heatmap_keeps_lookback_endpoints_shared_bounds_and_fractional_start() {
        let got = heatmap_from_points(
            vec![
                (vec![], vec![(900, 100), (1123, 100)]),
                (
                    vec![("env".into(), "b".into())],
                    vec![(1500, 40), (2123, 40)],
                ),
            ],
            1123,
            2123,
            1000,
            20,
        );
        check!(got.len() == 2);
        check!(got[0].slots[0].timestamp == 1123);
        check!(got[0].slots[0].counts[19] == 2);
        check!(got[1].slots[0].timestamp == 2123);
        check!(got[1].slots[0].counts[0] == 2);
        check!(got[0].slots[0].y_min == got[1].slots[0].y_min);
        check!(got[0].slots[0].y_min == (0..20).map(|n| f64::from(40 + n * 3)).collect::<Vec<_>>());
        check!(heatmap_from_points(vec![(vec![], vec![])], 1, 2, 1, 20).is_empty());
    }
}
