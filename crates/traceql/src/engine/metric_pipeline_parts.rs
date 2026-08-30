use super::*;

pub(crate) struct MetricPipelineParts<'a> {
    pub(crate) aggregate: Option<&'a Aggregate>,
    pub(crate) by: Vec<Field>,
    pub(crate) filter: Option<MetricFilter>,
    pub(crate) rank: Option<RankLimit>,
    pub(crate) compare: Option<CompareSpec>,
}

pub(crate) fn metric_pipeline_parts(pipeline: &[Pipeline]) -> Result<Option<MetricPipelineParts<'_>>> {
    let mut aggregate = None;
    let mut by = None;
    let mut filter = None;
    let mut rank = None;
    let mut compare = None;
    for stage in pipeline {
        match stage {
            Pipeline::Aggregate(value) if aggregate.is_none() => aggregate = Some(value),
            Pipeline::By(value) if by.is_none() => by = Some(value.clone()),
            Pipeline::Filter { op, value } if filter.is_none() => {
                filter = Some(metric_filter(*op, *value)?);
            }
            stage @ (Pipeline::TopK(_) | Pipeline::BottomK(_)) if rank.is_none() => {
                rank = Some(rank_limit(stage)?);
            }
            Pipeline::Compare {
                selection,
                top_n,
                start,
                end,
            } if compare.is_none() => {
                compare = Some(CompareSpec {
                    selection: (**selection).clone(),
                    top_n: *top_n,
                    start: start.map(UnixNano),
                    end: end.map(UnixNano),
                });
            }
            _ => return Ok(None),
        }
    }
    // A compare-only pipeline has no aggregate but is still a valid metric.
    if aggregate.is_none() && compare.is_none() {
        return Ok(None);
    }
    Ok(Some(MetricPipelineParts {
        aggregate,
        by: by.unwrap_or_default(),
        filter,
        rank,
        compare,
    }))
}
