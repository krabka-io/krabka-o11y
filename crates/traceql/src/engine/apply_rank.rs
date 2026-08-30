use super::*;

pub(crate) fn apply_rank(
    mut series: Vec<TraceMetricSeries>,
    rank: Option<RankLimit>,
) -> Vec<TraceMetricSeries> {
    let Some(rank) = rank else {
        return series;
    };
    series.sort_by(|a, b| {
        let a_score = series_rank_score(a);
        let b_score = series_rank_score(b);
        match rank.direction {
            RankDirection::Top => b_score
                .total_cmp(&a_score)
                .then_with(|| a.labels.cmp(&b.labels)),
            RankDirection::Bottom => a_score
                .total_cmp(&b_score)
                .then_with(|| a.labels.cmp(&b.labels)),
        }
    });
    series.truncate(rank.k);
    series
}
