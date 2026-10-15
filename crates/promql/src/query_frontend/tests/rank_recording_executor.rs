use super::*;

pub(super) fn ranked_series(name: &str, value: f64) -> RangeSeries {
    range_series(
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("series", name),
        ]),
        vec![RangeSeriesSample::new(0, SampleValue::Float(value))],
    )
}

#[derive(Default)]
pub(crate) struct RankRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RankRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("rank executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("rank query shard");
        let series = match shard.index {
            1 => vec![ranked_series("a", 10.0), ranked_series("b", 2.0)],
            2 => vec![ranked_series("c", 9.0), ranked_series("d", 8.0)],
            _ => Vec::new(),
        };
        Ok(unannotated(QueryResult::RangeMatrix(series)))
    }
}


mod rank_reduction_ranks_nan_last_for_top_and_bottom {
use super::*;
use super::*;

#[derive(Debug, PartialEq)]
struct SelectedValue {
    series: String,
    value: Option<f64>,
}

struct RankCase {
    reduction: RankReduction,
    k: usize,
    expected: Vec<SelectedValue>,
}

/// A `NaN` is neither a top value nor a bottom value. The sharded rank
/// reduction therefore drops it before it drops any number, which is what the
/// unsharded engine path does. A `topk` answered from shards then selects what
/// the same `topk` answered whole selects. `None` stands for the `NaN` sample
/// in the table below, because no float compares equal to a `NaN`.
#[test]
fn rank_reduction_ranks_nan_last_for_top_and_bottom() {
    use super::rank_recording_executor::ranked_series as series;
    let selected = |series: &str, value| SelectedValue {
        series: series.to_string(),
        value,
    };
    for case in [
        RankCase {
            reduction: RankReduction::Top,
            k: 1,
            expected: vec![selected("a", Some(10.0))],
        },
        RankCase {
            reduction: RankReduction::Bottom,
            k: 1,
            expected: vec![selected("b", Some(2.0))],
        },
        RankCase {
            reduction: RankReduction::Top,
            k: 3,
            expected: vec![
                selected("a", Some(10.0)),
                selected("b", Some(2.0)),
                selected("c", Some(9.0)),
            ],
        },
        RankCase {
            reduction: RankReduction::Bottom,
            k: 3,
            expected: vec![
                selected("a", Some(10.0)),
                selected("b", Some(2.0)),
                selected("c", Some(9.0)),
            ],
        },
        RankCase {
            reduction: RankReduction::Top,
            k: 4,
            expected: vec![
                selected("a", Some(10.0)),
                selected("b", Some(2.0)),
                selected("c", Some(9.0)),
                selected("n", None),
            ],
        },
    ] {
        let result = reduce_rank_range_query_results(
            QueryResult::RangeMatrix(vec![
                series("a", 10.0),
                series("b", 2.0),
                series("c", 9.0),
                series("n", f64::NAN),
            ]),
            case.k,
            case.reduction,
            None,
        )
        .unwrap();

        let QueryResult::RangeMatrix(kept) = result else {
            panic!("rank reduction range matrix");
        };
        let selected = kept
            .iter()
            .map(|series| {
                let SampleValue::Float(value) = &series.samples[0].value else {
                    panic!("rank reduction float sample");
                };
                SelectedValue {
                    series: series.labels.get("series").unwrap().to_owned(),
                    value: (!value.is_nan()).then_some(*value),
                }
            })
            .collect::<Vec<_>>();
        assert2::assert!(selected == case.expected);
    }
}
}
