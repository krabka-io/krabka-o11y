use num_traits::{FromPrimitive as _, ToPrimitive};

use crate::{
    ActiveLogDeleteFilter, BTreeMap, LabelIndex, Labels, METRIC_DECIMAL_SCALE, MetricValue,
    Ordering, Quantile, QueryError, QueryRow, StreamPlan, TimeRange, VectorAggregationOp,
    gcd_signed, is_deleted_log_entry, matching_loki_stream_entry,
};

// === split-modules: generated submodules ===
mod append_matching_log_row;
mod eval_times;
mod format_metric_value;
mod metric_sample_state;
mod metric_value;
mod rate_metric_value;
mod vector_aggregation_state;

pub(crate) use append_matching_log_row::append_matching_log_row;
pub(crate) use eval_times::eval_times;
pub(crate) use format_metric_value::format_metric_value;
pub(crate) use metric_sample_state::MetricSampleState;
pub(crate) use rate_metric_value::rate_metric_value;
pub(crate) use vector_aggregation_state::VectorAggregationState;
