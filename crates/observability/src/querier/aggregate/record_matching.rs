use crate::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, LabelIndex, Labels, MapArray, MetricQuery,
    MetricSamples, MetricValue, MetricWindow, PipelineStage, QueryError, RangeAggregation,
    SeriesFingerprint, StreamPlan, StreamQuery, StringArray, UNWRAP_SAMPLE_VALUE_LABEL,
    WalLogRecord, is_unwrapped_metric_query,
};

use datafusion::arrow::array::Array as _;

// === split-modules: generated submodules ===
mod append_matching_hot_log_record;
mod append_matching_hot_metric_record;
mod append_matching_metric_row;
mod is_deleted_log_entry;
mod matching_loki_metric_sample;
mod matching_loki_stream_entry;
mod parse_decimal_sample_exponent;
mod parse_decimal_sample_literal;
mod parse_metric_sample_value;
mod query_row;
mod should_insert_unknown_detected_level;
mod should_insert_unknown_detected_level_for_stream_query;
mod sort_loki_stream_values;
mod structured_metadata_value;

pub (crate) use append_matching_hot_log_record::append_matching_hot_log_record;
pub (crate) use append_matching_hot_metric_record::append_matching_hot_metric_record;
pub (crate) use append_matching_metric_row::append_matching_metric_row;
pub (crate) use is_deleted_log_entry::is_deleted_log_entry;
pub (crate) use matching_loki_metric_sample::matching_loki_metric_sample;
pub (crate) use matching_loki_stream_entry::matching_loki_stream_entry;
pub (crate) use parse_decimal_sample_exponent::parse_decimal_sample_exponent;
pub (crate) use parse_decimal_sample_literal::parse_decimal_sample_literal;
pub (crate) use parse_metric_sample_value::parse_metric_sample_value;
pub (crate) use query_row::QueryRow;
pub (crate) use should_insert_unknown_detected_level::should_insert_unknown_detected_level;
pub (crate) use should_insert_unknown_detected_level_for_stream_query::should_insert_unknown_detected_level_for_stream_query;
pub (crate) use sort_loki_stream_values::sort_loki_stream_values;
pub (crate) use structured_metadata_value::structured_metadata_value;
