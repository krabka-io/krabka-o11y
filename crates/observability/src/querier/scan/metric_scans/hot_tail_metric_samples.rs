use std::borrow::Borrow;

use super::{
    MetricQuery, MetricWindow, QueryError, QueryHotTail, StreamPlan, WalLogRecord,
    append_matching_hot_metric_record, apply_absent_over_time, format_metric_samples,
};
use crate::{FormattedMetricSeries, MetricSamples};

/// The WAL hot tail of a metric range query, and what its records are
/// matched and windowed against.
pub(crate) struct HotTailMetricSamples<'a, R> {
    pub(crate) plan: &'a StreamPlan,
    pub(crate) query: &'a MetricQuery,
    pub(crate) eval_times: &'a [i64],
    pub(crate) hot_tail: QueryHotTail<'a, R>,
}

impl<R: Borrow<WalLogRecord>> HotTailMetricSamples<'_, R> {
    /// Adds the hot tail's matching records to the blocks' `samples`, fills
    /// in what `absent_over_time` reports, and formats the series.
    pub(crate) fn merge_into(
        self,
        mut samples: MetricSamples,
    ) -> Result<FormattedMetricSeries, QueryError> {
        let Self {
            plan,
            query,
            eval_times,
            hot_tail,
        } = self;
        for record in hot_tail.records {
            let record: &WalLogRecord = record.borrow();
            append_matching_hot_metric_record(
                &mut samples,
                plan,
                record,
                hot_tail.frontier,
                MetricWindow {
                    query,
                    eval_times,
                    range_ns: query.range_ns.0,
                    delete_filters: hot_tail.delete_filters,
                },
            )?;
        }
        apply_absent_over_time(&mut samples, query, eval_times);
        Ok(format_metric_samples(samples, query))
    }
}
