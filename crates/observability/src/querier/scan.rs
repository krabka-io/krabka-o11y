use crate::{
    LabelIndex, MetricQuery, QueryError, QueryHotTail, StreamPlan, TimeRange, checked_eval_times,
};

pub(crate) struct MetricRangeScanRequest<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) label_index: &'a LabelIndex,
    pub(crate) evaluation: MetricEvaluationRange,
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricEvaluationRange {
    pub range: TimeRange,
    pub step_ns: i64,
}

impl MetricEvaluationRange {
    pub const fn new(range: TimeRange, step_ns: i64) -> Self {
        Self { range, step_ns }
    }
}

pub(crate) struct PreparedMetricRangeScan<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) label_index: &'a LabelIndex,
    pub(crate) eval_range: TimeRange,
    pub(crate) eval_times: Vec<i64>,
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

struct MetricEvaluationTimes {
    range: TimeRange,
    times: Vec<i64>,
}

impl<'a, R> MetricRangeScanRequest<'a, R> {
    pub(crate) fn new(
        plan: &'a StreamPlan,
        query: &'a MetricQuery,
        label_index: &'a LabelIndex,
        evaluation: MetricEvaluationRange,
        hot_tail: QueryHotTail<'a, R>,
    ) -> Self {
        Self {
            plan,
            query,
            label_index,
            evaluation,
            hot_tail,
        }
    }

    pub(crate) fn prepare(self) -> Result<PreparedMetricRangeScan<'a, R>, QueryError> {
        let Self {
            plan,
            query,
            label_index,
            evaluation,
            hot_tail,
        } = self;
        let evaluation_times = metric_range_scan_eval_times(evaluation)?;
        Ok(PreparedMetricRangeScan {
            plan,
            query,
            label_index,
            eval_range: evaluation_times.range,
            eval_times: evaluation_times.times,
            hot_tail,
        })
    }
}

fn metric_range_scan_eval_times(
    evaluation: MetricEvaluationRange,
) -> Result<MetricEvaluationTimes, QueryError> {
    Ok(MetricEvaluationTimes {
        range: evaluation.range,
        times: checked_eval_times(evaluation.range, evaluation.step_ns)?,
    })
}

pub(crate) mod stream_scans;
pub use stream_scans::{
    execute_stream_query, execute_stream_query_from_object_store,
    execute_stream_query_with_hot_tail, execute_stream_query_with_hot_tail_frontier,
    metric_plan_scan_sql, stream_plan_scan_sql,
};
pub(crate) mod metric_scans;
pub use metric_scans::{
    execute_metric_query, execute_metric_query_from_object_store, execute_metric_query_range,
    execute_metric_query_range_from_object_store, execute_metric_query_range_with_hot_tail,
    execute_metric_query_range_with_hot_tail_frontier, execute_metric_query_with_hot_tail,
    execute_metric_query_with_hot_tail_frontier, execute_tail_query,
    execute_tail_query_with_frontier,
};
pub(crate) mod object_store_scans;
