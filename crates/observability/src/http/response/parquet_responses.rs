use axum::response::IntoResponse;
use krabka_units::convert::ByteSizeExt;

use crate::{
    ActiveLogDeleteFilter, ArrowWriter, BTreeMap, BlockDescriptor, ByteSize, CompactionFrontier,
    HttpQueryError, LOKI_PARQUET_CONTENT_TYPE, Labels, MapArray, MapBuilder, MetricQuery,
    MetricWindow, RangeAggregation, RecordBatch, Response, StatusCode, StreamPlan, StringBuilder,
    TimeRange, Value, WalLogRecord, append_matching_hot_metric_record, eval_times,
    format_metric_samples, json, json_response, loki_query_stats, matching_loki_stream_entry,
    unix_ns_string_to_loki_seconds,
};

// === split-modules: generated submodules ===
mod add_loki_query_stat_field;
mod add_loki_query_stats;
mod add_loki_query_stats_for_metric_plan;
mod add_loki_query_stats_for_metric_plan_with_hot_tail;
mod add_loki_query_stats_for_stream_blocks_with_hot_tail;
mod add_loki_query_stats_for_stream_plan;
mod add_loki_query_stats_for_stream_plan_with_hot_tail;
mod consume_hot_metric_sample;
mod count_loki_metric_result_hot_tail_samples;
mod count_loki_metric_result_samples;
mod count_loki_metric_result_scan_lines;
mod count_loki_stream_result_hot_tail_lines;
mod count_loki_stream_result_lines;
mod json_object_to_labels;
mod loki_metric_sample_timestamp_key;
mod loki_parquet_batch_response;
mod loki_parquet_label_array;
mod loki_sparse_success;
mod loki_success;
mod loki_success_value;
mod merge_loki_query_response;
mod merge_loki_query_stats;
mod planned_block_bytes;
mod planned_block_bytes_for_blocks;
mod populate_loki_query_scan_stats;

pub (crate) use add_loki_query_stat_field::add_loki_query_stat_field;
pub (crate) use add_loki_query_stats::add_loki_query_stats;
pub (crate) use add_loki_query_stats_for_metric_plan::add_loki_query_stats_for_metric_plan;
pub (crate) use add_loki_query_stats_for_metric_plan_with_hot_tail::add_loki_query_stats_for_metric_plan_with_hot_tail;
pub (crate) use add_loki_query_stats_for_stream_blocks_with_hot_tail::add_loki_query_stats_for_stream_blocks_with_hot_tail;
pub (crate) use add_loki_query_stats_for_stream_plan::add_loki_query_stats_for_stream_plan;
pub (crate) use add_loki_query_stats_for_stream_plan_with_hot_tail::add_loki_query_stats_for_stream_plan_with_hot_tail;
pub (crate) use consume_hot_metric_sample::consume_hot_metric_sample;
pub (crate) use count_loki_metric_result_hot_tail_samples::count_loki_metric_result_hot_tail_samples;
pub (crate) use count_loki_metric_result_samples::count_loki_metric_result_samples;
pub (crate) use count_loki_metric_result_scan_lines::count_loki_metric_result_scan_lines;
pub (crate) use count_loki_stream_result_hot_tail_lines::count_loki_stream_result_hot_tail_lines;
pub (crate) use count_loki_stream_result_lines::count_loki_stream_result_lines;
pub (crate) use json_object_to_labels::json_object_to_labels;
pub (crate) use loki_metric_sample_timestamp_key::loki_metric_sample_timestamp_key;
pub (crate) use loki_parquet_batch_response::loki_parquet_batch_response;
pub (crate) use loki_parquet_label_array::loki_parquet_label_array;
pub (crate) use loki_sparse_success::loki_sparse_success;
pub (crate) use loki_success::loki_success;
pub (crate) use loki_success_value::loki_success_value;
pub (crate) use merge_loki_query_response::merge_loki_query_response;
pub (crate) use merge_loki_query_stats::merge_loki_query_stats;
pub (crate) use planned_block_bytes::planned_block_bytes;
pub (crate) use planned_block_bytes_for_blocks::planned_block_bytes_for_blocks;
pub (crate) use populate_loki_query_scan_stats::populate_loki_query_scan_stats;
