use std::collections::{BTreeMap, BTreeSet};

use super::{RankDirection, RankLimit, TraceMetricSeries};

struct RankedValue {
    series_index: usize,
    value: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RankedPointIdentity {
    series_index: usize,
    timestamp: i64,
}

pub(crate) fn apply_rank(
    mut series: Vec<TraceMetricSeries>,
    rank: Option<RankLimit>,
) -> Vec<TraceMetricSeries> {
    let Some(rank) = rank else {
        return series;
    };
    // Tempo ranks independently at every timestamp, retaining the union of
    // winning series. Exemplars belong to that series, not to its rank points.
    let mut timestamps = BTreeMap::<i64, Vec<RankedValue>>::new();
    for (series_index, series_item) in series.iter().enumerate() {
        for point in &series_item.points {
            if !point.value.is_nan() {
                timestamps
                    .entry(point.timestamp)
                    .or_default()
                    .push(RankedValue {
                        series_index,
                        value: point.value,
                    });
            }
        }
    }
    let mut admitted = BTreeSet::new();
    for entry in timestamps {
        let timestamp = entry.0;
        let mut values = entry.1;
        values.sort_by(|left, right| {
            // partial_cmp equates both signed zeros, as Go's == does.
            let order = left
                .value
                .partial_cmp(&right.value)
                .expect("NaN samples were excluded");
            let labels = series[left.series_index]
                .labels
                .cmp(&series[right.series_index].labels);
            match rank.direction {
                RankDirection::Top => order.reverse().then(labels),
                RankDirection::Bottom => order.then(labels.reverse()),
            }
        });
        admitted.extend(
            values
                .into_iter()
                .take(rank.k)
                .map(|value| RankedPointIdentity {
                    series_index: value.series_index,
                    timestamp,
                }),
        );
    }
    for (series_index, series_item) in series.iter_mut().enumerate() {
        series_item.points.retain(|point| {
            admitted.contains(&RankedPointIdentity {
                series_index,
                timestamp: point.timestamp,
            })
        });
    }
    series.retain(|series| !series.points.is_empty());
    series
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::{RankDirection, RankLimit, TraceMetricSeries, apply_rank};
    use crate::{MetricPoint, result::MetricLabel};

    fn series(name: &str, points: Vec<MetricPoint>) -> TraceMetricSeries {
        TraceMetricSeries {
            labels: vec![MetricLabel::new("name", name)],
            label_types: BTreeMap::new(),
            points,
            exemplars: Vec::new(),
        }
    }

    #[test]
    fn crossing_winners_are_ranked_per_timestamp_and_nan_never_wins() {
        let input = vec![
            series(
                "a",
                vec![MetricPoint::new(0, 9.0), MetricPoint::new(1, 1.0)],
            ),
            series(
                "b",
                vec![MetricPoint::new(0, 2.0), MetricPoint::new(1, 8.0)],
            ),
            series(
                "c",
                vec![MetricPoint::new(0, f64::NAN), MetricPoint::new(1, f64::NAN)],
            ),
        ];
        assert!(
            apply_rank(
                input,
                Some(RankLimit {
                    direction: RankDirection::Top,
                    k: 1
                })
            ) == vec![
                series("a", vec![MetricPoint::new(0, 9.0)]),
                series("b", vec![MetricPoint::new(1, 8.0)])
            ]
        );
    }

    #[test]
    fn chained_rank_order_and_signed_zero_ties_match_pinned_heap() {
        let input = vec![
            series("a", vec![MetricPoint::new(0, 3.0)]),
            series("b", vec![MetricPoint::new(0, 2.0)]),
            series("c", vec![MetricPoint::new(0, 1.0)]),
        ];
        let top = RankLimit {
            direction: RankDirection::Top,
            k: 2,
        };
        let bottom = RankLimit {
            direction: RankDirection::Bottom,
            k: 1,
        };
        assert!(
            apply_rank(apply_rank(input.clone(), Some(top)), Some(bottom))
                == vec![series("b", vec![MetricPoint::new(0, 2.0)])]
        );
        assert!(
            apply_rank(apply_rank(input, Some(bottom)), Some(top))
                == vec![series("c", vec![MetricPoint::new(0, 1.0)])]
        );
        let zeros = vec![
            series("a", vec![MetricPoint::new(0, -0.0)]),
            series("b", vec![MetricPoint::new(0, 0.0)]),
        ];
        assert!(
            apply_rank(
                zeros.clone(),
                Some(RankLimit {
                    direction: RankDirection::Top,
                    k: 1
                })
            ) == vec![series("a", vec![MetricPoint::new(0, -0.0)])]
        );
        assert!(
            apply_rank(zeros, Some(bottom)) == vec![series("b", vec![MetricPoint::new(0, 0.0)])]
        );
    }
}
