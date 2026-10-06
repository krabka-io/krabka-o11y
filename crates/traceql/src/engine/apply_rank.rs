use std::collections::{BTreeMap, BTreeSet};

use super::{RankDirection, RankLimit, TraceMetricSeries};

pub(crate) fn apply_rank(
    mut series: Vec<TraceMetricSeries>,
    rank: Option<RankLimit>,
) -> Vec<TraceMetricSeries> {
    let Some(rank) = rank else {
        return series;
    };
    // Tempo ranks independently at every timestamp, retaining the union of
    // winning series. Exemplars belong to that series, not to its rank points.
    let mut timestamps = BTreeMap::<i64, Vec<(usize, f64)>>::new();
    for (index, series) in series.iter().enumerate() {
        for (timestamp, value) in &series.points {
            if !value.is_nan() {
                timestamps
                    .entry(*timestamp)
                    .or_default()
                    .push((index, *value));
            }
        }
    }
    let mut admitted = BTreeSet::new();
    for (timestamp, mut values) in timestamps {
        values.sort_by(|(left, a), (right, b)| {
            // partial_cmp equates both signed zeros, as Go's == does.
            let order = a.partial_cmp(b).expect("NaN samples were excluded");
            let labels = series[*left].labels.cmp(&series[*right].labels);
            match rank.direction {
                RankDirection::Top => order.reverse().then(labels),
                RankDirection::Bottom => order.then(labels.reverse()),
            }
        });
        admitted.extend(
            values
                .into_iter()
                .take(rank.k)
                .map(|(index, _)| (index, timestamp)),
        );
    }
    for (index, series) in series.iter_mut().enumerate() {
        series
            .points
            .retain(|(timestamp, _)| admitted.contains(&(index, *timestamp)));
    }
    series.retain(|series| !series.points.is_empty());
    series
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::{RankDirection, RankLimit, TraceMetricSeries, apply_rank};

    fn series(name: &str, points: Vec<(i64, f64)>) -> TraceMetricSeries {
        TraceMetricSeries {
            labels: vec![("name".into(), name.into())],
            label_types: BTreeMap::new(),
            points,
            exemplars: Vec::new(),
        }
    }

    #[test]
    fn crossing_winners_are_ranked_per_timestamp_and_nan_never_wins() {
        let input = vec![
            series("a", vec![(0, 9.0), (1, 1.0)]),
            series("b", vec![(0, 2.0), (1, 8.0)]),
            series("c", vec![(0, f64::NAN), (1, f64::NAN)]),
        ];
        assert!(
            apply_rank(
                input,
                Some(RankLimit {
                    direction: RankDirection::Top,
                    k: 1
                })
            ) == vec![series("a", vec![(0, 9.0)]), series("b", vec![(1, 8.0)])]
        );
    }

    #[test]
    fn chained_rank_order_and_signed_zero_ties_match_pinned_heap() {
        let input = vec![
            series("a", vec![(0, 3.0)]),
            series("b", vec![(0, 2.0)]),
            series("c", vec![(0, 1.0)]),
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
                == vec![series("b", vec![(0, 2.0)])]
        );
        assert!(
            apply_rank(apply_rank(input, Some(bottom)), Some(top))
                == vec![series("c", vec![(0, 1.0)])]
        );
        let zeros = vec![series("a", vec![(0, -0.0)]), series("b", vec![(0, 0.0)])];
        assert!(
            apply_rank(
                zeros.clone(),
                Some(RankLimit {
                    direction: RankDirection::Top,
                    k: 1
                })
            ) == vec![series("a", vec![(0, -0.0)])]
        );
        assert!(apply_rank(zeros, Some(bottom)) == vec![series("b", vec![(0, 0.0)])]);
    }
}
