use crate::{
    ActiveLogDeleteFilter, BTreeMap, Int64Array, LabelIndex, Labels, MapArray, MatchOp,
    MetricQuery, MetricSampleState, Ordering, PipelineStage, QueryError, QueryRow,
    RangeAggregation, StreamPlan, StringArray, UInt64Array, VectorAggregation, VectorAggregationOp,
    VectorAggregationState, VectorGrouping, append_matching_metric_row, format_metric_value,
    parse_metric_sample_value, rate_metric_value, structured_metadata_value,
};

// === split-modules: generated submodules ===
mod absent_metric_labels;
mod aggregate_vector_samples;
mod apply_absent_over_time;
mod count_values_vector_samples;
mod format_metric_samples;
mod formatted_metric_series;
mod group_range_samples;
mod is_unwrapped_metric_query;
mod merge_metric_samples;
mod metric_decimal_scale;
mod metric_samples;
mod metric_samples_from_batches;
mod metric_value;
mod metric_window;
mod range_sample_value;
mod select_all_vector_samples;
mod select_vector_samples;
mod sort_formatted_vector_samples;
mod vector_group_labels;
mod vector_selection;

pub (crate) use absent_metric_labels::absent_metric_labels;
pub (crate) use aggregate_vector_samples::aggregate_vector_samples;
pub (crate) use apply_absent_over_time::apply_absent_over_time;
pub (crate) use count_values_vector_samples::count_values_vector_samples;
pub (crate) use format_metric_samples::format_metric_samples;
pub (crate) use formatted_metric_series::FormattedMetricSeries;
pub (crate) use group_range_samples::group_range_samples;
pub (crate) use is_unwrapped_metric_query::is_unwrapped_metric_query;
pub (crate) use merge_metric_samples::merge_metric_samples;
pub (crate) use metric_decimal_scale::METRIC_DECIMAL_SCALE;
pub (crate) use metric_samples::MetricSamples;
pub (crate) use metric_samples_from_batches::metric_samples_from_batches;
pub (crate) use metric_value::MetricValue;
pub (crate) use metric_window::MetricWindow;
pub (crate) use range_sample_value::range_sample_value;
pub (crate) use select_all_vector_samples::select_all_vector_samples;
pub (crate) use select_vector_samples::select_vector_samples;
pub (crate) use sort_formatted_vector_samples::sort_formatted_vector_samples;
pub (crate) use vector_group_labels::vector_group_labels;
pub (crate) use vector_selection::VectorSelection;
