use crate::{
    Arc, BTreeMap, LabelIndex, MetricQuery, MetricWindow, ObjectPath, ObjectStore, QueryError,
    QueryHotTail, StreamPlan, TimeRange, Value, append_matching_hot_metric_record,
    apply_absent_over_time, collect_object_store_metric_log_batches, eval_times,
    format_metric_samples, loki_matrix_response_with_warnings, merge_metric_samples,
    metric_samples_from_batches,
};

// === split-modules: generated submodules ===
mod execute_metric_query_range_from_object_store_with_hot_tail_frontier;
mod execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes;

pub (crate) use execute_metric_query_range_from_object_store_with_hot_tail_frontier::execute_metric_query_range_from_object_store_with_hot_tail_frontier;
pub (crate) use execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes::execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes;
