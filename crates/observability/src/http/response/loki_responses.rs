use datafusion::arrow::array::Array as _;
use num_traits::FromPrimitive as _;

use crate::{
    ACCEPT, Arc, ArrayRef, BTreeMap, DataType, Duration, Field, Float64Array,
    FormattedMetricSeries, HeaderMap, HttpQueryError, Labels, LokiDirection, MetricValue,
    RecordBatch, Response, Schema, StringArray, TimeUnit, TimestampNanosecondArray, Value, json,
    loki_parquet_batch_response, loki_parquet_label_array, loki_success_value,
    parse_metric_sample_value,
};

// === split-modules: generated submodules ===
mod accept_parameter_is_zero_quality;
mod accept_part_allows_loki_parquet;
mod apply_loki_stream_end_bound;
mod apply_loki_stream_interval;
mod apply_loki_stream_limit;
mod apply_loki_stream_options;
mod loki_matrix_response;
mod loki_matrix_response_with_warnings;
mod loki_metric_parquet_kind;
mod loki_metric_sample;
mod loki_metrics_parquet_response;
mod loki_parquet_content_type;
mod loki_parquet_labels;
mod loki_parquet_metric_sample;
mod loki_parquet_metric_timestamp_ns;
mod loki_parquet_response;
mod loki_streams_parquet_response;
mod loki_streams_response;
mod loki_streams_response_with_warnings;
mod loki_vector_response_from_matrix;
mod unix_ns_string_to_loki_seconds;
mod wants_loki_parquet;

pub(crate) use accept_parameter_is_zero_quality::accept_parameter_is_zero_quality;
pub(crate) use accept_part_allows_loki_parquet::accept_part_allows_loki_parquet;
pub(crate) use apply_loki_stream_end_bound::apply_loki_stream_end_bound;
pub(crate) use apply_loki_stream_interval::apply_loki_stream_interval;
pub(crate) use apply_loki_stream_limit::apply_loki_stream_limit;
pub(crate) use apply_loki_stream_options::apply_loki_stream_options;
pub(crate) use loki_matrix_response::loki_matrix_response;
pub(crate) use loki_matrix_response_with_warnings::loki_matrix_response_with_warnings;
pub(crate) use loki_metric_parquet_kind::LokiMetricParquetKind;
pub(crate) use loki_metric_sample::loki_metric_sample;
pub(crate) use loki_metrics_parquet_response::loki_metrics_parquet_response;
pub(crate) use loki_parquet_content_type::LOKI_PARQUET_CONTENT_TYPE;
pub(crate) use loki_parquet_labels::loki_parquet_labels;
pub(crate) use loki_parquet_metric_sample::loki_parquet_metric_sample;
pub(crate) use loki_parquet_metric_timestamp_ns::loki_parquet_metric_timestamp_ns;
pub(crate) use loki_parquet_response::loki_parquet_response;
pub(crate) use loki_streams_parquet_response::loki_streams_parquet_response;
pub(crate) use loki_streams_response::loki_streams_response;
pub(crate) use loki_streams_response_with_warnings::loki_streams_response_with_warnings;
pub(crate) use loki_vector_response_from_matrix::loki_vector_response_from_matrix;
pub(crate) use unix_ns_string_to_loki_seconds::unix_ns_string_to_loki_seconds;
pub(crate) use wants_loki_parquet::wants_loki_parquet;
