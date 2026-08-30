//! Role-selectable service skeleton for Krabka observability.

pub mod ids;
pub mod metrics;

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    future::pending,
    io::ErrorKind,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{
        Path, RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{
        HeaderMap, StatusCode,
        header::{ACCEPT, CONTENT_ENCODING, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::{Parser, ValueEnum};
use datafusion::{
    arrow::{
        array::{
            ArrayRef, Float64Array, Int64Array, MapArray, StringArray, TimestampNanosecondArray,
            UInt64Array,
            builder::{MapBuilder, StringBuilder},
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    prelude::SessionContext,
};
use flate2::read::{DeflateDecoder, GzDecoder};
pub use ids::{Offset, PartitionIndex};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex,
    LogBlockStoreError as BlockStoreError, LogLabels as Labels, LogRow,
    LogSeriesFingerprint as SeriesFingerprint, TimeRange, read_log_block,
    read_log_block_from_object_store, read_log_index_manifest,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store, register_log_blocks,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};
use krabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, AdminClient, AdminError, PatternType, PermissionType,
    ResourceType,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError};
use krabka_client_producer::{
    Acks, Header as ProducerHeader, Producer, ProducerError, ProducerRecord,
};
use krabka_logql::{
    ComparisonOp, FieldFilter, FieldFilterExpression, FieldFilterLogicOp, FieldValue,
    LabelFormatValue, LabelSelectionMatcher, LabelSelectionSet, LineFilterOp, LogfmtParserConfig,
    MatchOp, MetricBinaryArithmetic, MetricBinaryComparison, MetricBinarySet, MetricBinarySetOp,
    MetricLabelJoin, MetricQuery, MetricScalarArithmetic, MetricScalarArithmeticOp,
    MetricScalarComparison, MetricVectorGroupModifier, MetricVectorMatching, ParseError,
    ParserStage, PipelineStage, PlanError, Quantile, RangeAggregation, StreamPlan, StreamQuery,
    UNWRAP_SAMPLE_VALUE_LABEL, UnwrapConversion, VectorAggregation, VectorAggregationOp,
    VectorGrouping, parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query, plan_stream_query,
};
use krabka_units::{
    ByteRate, ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
    days, hours, millis, minutes, secs,
};
use object_store::{ObjectStore, local::LocalFileSystem, parse_url_opts, path::Path as ObjectPath};
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        ExportLogsServiceRequest as ProtoExportLogsServiceRequest,
        ExportLogsServiceResponse as ProtoExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    common::v1::{
        AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue, any_value as proto_any_value,
    },
    logs::v1::LogRecord as ProtoLogRecord,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snap::raw::Decoder as SnappyDecoder;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::metrics::ServiceMetrics;

mod compactor;
mod config;
mod deletes;
mod deletes_api;
mod distributor;
mod error;
mod http;
mod querier;
mod ruler;
mod service;
mod service_runtime;
mod wal;

pub use compactor::{
    CompactionCommitError, CompactionError, CompactionFrontier, CompactionFrontierStoreError,
    CompactionOffsetCommitter, CompactorRunError, KafkaWalCompactionError, KafkaWalHeader,
    KafkaWalRecord, SharedCompactionFrontier, WalLogRecord, WalPosition,
    build_service_dependencies, build_service_dependencies_with_client_resource_policy,
    compact_kafka_wal_records_to_object_store, compact_log_block_to_object_store,
    compact_next_kafka_wal_batch_to_object_store, compact_wal_records_to_object_store,
    read_compaction_frontier_from_object_store, run_compactor_once, run_compactor_until_idle,
    run_compactor_until_shutdown, write_compaction_frontier_to_object_store,
};
pub use config::{
    QuerierIndexSource, Role, ServiceConfig, ServiceConfigError, ServiceRuntimeError,
};
pub(crate) use deletes::{
    ActiveLogDeleteFilter, CompactorDeleteRequest, CompactorDeleteRequestResponse,
    CompactorDeleteRequests, CompactorDeleteState, CreateDeleteRequestParams,
    ListDeleteRequestsParams,
};
pub(crate) use deletes_api::{
    active_log_delete_filters, active_log_delete_filters_from_requests, cancel_delete_request,
    create_delete_request, list_delete_requests,
};
pub use distributor::{
    DistributorState, OtlpGrpcLogsService, distributor_router, otlp_grpc_logs_service,
    otlp_grpc_logs_service_with_limiter,
};
pub use error::QueryError;
pub use http::loki_router;
pub use querier::{
    QuerierState, build_querier_state, execute_metric_query,
    execute_metric_query_from_object_store, execute_metric_query_range,
    execute_metric_query_range_from_object_store, execute_metric_query_range_with_hot_tail,
    execute_metric_query_range_with_hot_tail_frontier, execute_metric_query_with_hot_tail,
    execute_metric_query_with_hot_tail_frontier, execute_stream_query,
    execute_stream_query_from_object_store, execute_stream_query_with_hot_tail,
    execute_stream_query_with_hot_tail_frontier, execute_tail_query,
    execute_tail_query_with_frontier, metric_plan_scan_sql, stream_plan_scan_sql,
};
pub use service::{
    ActiveLogDeleteFilterError, ClientResourcePolicy, LogDeleteRequestStoreError,
    LokiRuleStoreError, ServiceDependencies, ServiceStatus, SharedLogDeleteRequests, run,
};
pub use service_runtime::{
    build_service_router, serve_service, serve_service_listener, shutdown_signal,
};
pub use wal::{
    BufferedLogHotTail, HotTailPollError, InMemoryWalSink, IngestLimitError, KafkaLogWalConsumer,
    KafkaLogWalSink, LogHotTail, LogIngestLimiter, LogQueryAuthorizer, LogWalConsumer, LogWalSink,
    QueryAuthorizationError, WalConsumerError, WalRecordDecodeError, WalSinkError,
    build_kafka_wal_record, decode_kafka_wal_record, decode_kafka_wal_record_envelope,
    poll_log_hot_tail_once,
};

#[cfg(test)]
mod tests;

pub(crate) use self::{
    compactor::{
        configuration::{
            build_compactor_configured_object_store, compactor_object_store,
            validate_compactor_policy,
        },
        delete_materialization::{
            LogCompactionIndexOutput, TenantCompactionIndexCache, active_log_delete_tenants,
            compact_log_block_to_object_store_with_index_output,
            materialize_delete_requests_in_existing_local_manifest_blocks,
            materialize_delete_requests_in_existing_object_store_blocks,
            poll_accumulated_log_compaction_records, wal_compaction_chunks, wal_record_time_range,
        },
        frontier::{
            CompactionFrontierRefreshSource, CompactionFrontierSource, ConfiguredObjectStore,
            LastCompactedPosition,
            compact_wal_records_to_object_store_with_delete_filters_and_index_output,
        },
        object_store_support::{
            build_configured_object_store, compactor_delete_requests_for_config,
            load_querier_shared_compaction_frontier,
        },
        runtime::{
            advance_and_persist_compaction_frontier, load_existing_compaction_frontier,
            materialize_deletes_then_compact_next_kafka_wal_batch,
            shared_compaction_frontier_from_object_store, spawn_compaction_frontier_refresher,
        },
    },
    config::LOKI_REJECT_OLD_SAMPLES_MAX_AGE,
    distributor::{
        ingest::{
            append_distributor_wal_records, loki_decode_error_context, measured_size,
            normalize_loki_http_push, normalize_otlp_http_logs, otlp_http_error_response,
            record_ingest_response, validate_ingest_body_limit,
        },
        loki_normalization::{
            is_loki_json_content_type, is_loki_label_name, is_protobuf_content_type,
            loki_json_timestamp_value_parse_error, normalize_loki_proto_push, normalize_loki_push,
            normalize_otlp_logs,
        },
        otlp_normalization::{
            detect_log_level, discover_detected_level_label, discover_service_name_label,
            loki_missing_proto_timestamp_error, loki_proto_label_pairs_to_labels,
            loki_proto_timestamp_ns, loki_stale_sample_label_set, normalize_otlp_proto_logs,
            normalize_otlp_proto_logs_for_tenant, otlp_attributes_to_labels,
            otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string,
            rfc3339_seconds, validate_ingest_timestamp_ns, validate_loki_timestamp_window,
        },
        router::{
            COMPACTOR_OPS, LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoTimestamp,
            LokiPushRequest, LokiTypedPushRequest, OtlpAnyValue, OtlpKeyValue, OtlpLogRecord,
            OtlpLogsRequest, QUERIER_OPS, RoleOps, ServiceReadiness, distributor_router_with_sink,
            with_role_ops_routes,
        },
        value_conversion::{
            hex_string, metadata_value_to_string, otlp_value_to_json, parse_structured_metadata,
            proto_value_to_string,
        },
    },
    error::query_errors::{
        DistributorError, HttpQueryError, distributor_error_to_grpc_status, json_response,
        loki_error, loki_format_query_invalid_response, loki_parse_error, loki_parse_error_text,
        text_response,
    },
    http::{
        handlers::{
            metadata_handlers::{
                api_prom_label_names, api_prom_label_names_post, api_prom_label_values,
                api_prom_label_values_post, api_prom_series, api_prom_series_post,
                handle_api_prom_query, handle_api_prom_query_range, handle_query, index_stats,
                index_stats_post, index_volume, index_volume_post, index_volume_range,
                index_volume_range_post, label_values, label_values_post, series, series_post,
                tail,
            },
            query_execution::execute_http_query_for_tenant,
            request_types::{
                DetectedFieldStats, DetectedFieldType, DetectedFieldsParams, DetectedLabelsParams,
                PatternsParams, QueryParams, SeriesParams, VolumeAggregateBy, VolumeKind,
                VolumeParams, api_prom_query, api_prom_query_post, api_prom_query_range,
                api_prom_query_range_post, build_info, detected_field_values,
                detected_field_values_post, detected_fields, detected_fields_post, detected_labels,
                detected_labels_post, format_query, format_query_post, label_names,
                label_names_post, patterns, patterns_post, query, query_post, query_range,
                query_range_post, status_metrics,
            },
        },
        params::{
            query_parsing::{
                parse_detected_fields_params, parse_detected_labels_params,
                parse_loki_duration_query_param, parse_loki_timestamp_query_param,
                parse_patterns_params, parse_prometheus_duration, parse_query_params,
                parse_volume_params, split_query_param_pairs, validate_loki_tail_delay_for,
            },
            value_decoding::{
                LOKI_DEFAULT_QUERY_RANGE, LOKI_DEFAULT_TAIL_LIMIT,
                LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS, LOKI_MAX_TAIL_DELAY,
                LOKI_METADATA_DEFAULT_INDEX_RANGE, LOKI_VOLUME_MAX_QUERY_RANGE, LokiDirection,
                QueryKind, authorized_tenant, authorized_tenants, current_unix_time_ns,
                decode_form_component, grpc_tenant, loki_direction, optional_start_end_range,
                parse_decimal_seconds_timestamp, parse_usize_query_param, start_or_since, tenant,
                time_range,
            },
        },
        params_format::{
            aggregation_formatting::{
                FormattedVectorBinaryModifiers, format_loki_duration_ns,
                format_loki_offset_duration_ns, format_quantile, format_range_aggregation_name,
                format_scalar_vector_expression, format_vector_aggregation_query,
                format_vector_function_text, format_vector_grouping,
                format_vector_label_replace_function, parse_logql_string_argument,
                split_logql_function_arguments,
            },
            metric_formatting::{
                format_label_replace_metric_scalar_expression,
                format_label_replace_metric_vector_expression, format_logql_quoted_string,
                format_metric_label_replace_query, format_metric_query,
                format_metric_scalar_arithmetic_expression,
                format_metric_scalar_arithmetic_operator,
                format_metric_scalar_comparison_expression,
                format_metric_scalar_comparison_operator,
                format_metric_vector_comparison_expression, format_metric_vector_set_expression,
                format_simple_metric_query, format_sort_vector_expression, indent_logql_lines,
                split_top_level_arithmetic_query, split_top_level_comparison_query,
                split_top_level_set_query,
            },
            request_formatting::{
                execute_format_query, form_body_query, format_metric_vector_arithmetic_expression,
                format_metric_vector_binary_expression, post_query_params,
                post_query_params_body_first, split_leading_vector_binary_modifiers,
            },
            stream_formatting::{
                format_stream_query, parse_formatted_vector_function,
                parse_vector_arithmetic_operator, quote_logql_string, validate_query_bytes_limit,
                validate_query_series_limit,
            },
        },
        response::{
            loki_responses::{
                LOKI_PARQUET_CONTENT_TYPE, apply_loki_stream_options, loki_matrix_response,
                loki_matrix_response_with_warnings, loki_parquet_response, loki_streams_response,
                loki_streams_response_with_warnings, loki_vector_response_from_matrix,
                unix_ns_string_to_loki_seconds, wants_loki_parquet,
            },
            parquet_responses::{
                add_loki_query_stats, add_loki_query_stats_for_metric_plan,
                add_loki_query_stats_for_metric_plan_with_hot_tail,
                add_loki_query_stats_for_stream_blocks_with_hot_tail,
                add_loki_query_stats_for_stream_plan,
                add_loki_query_stats_for_stream_plan_with_hot_tail, json_object_to_labels,
                loki_parquet_batch_response, loki_parquet_label_array, loki_sparse_success,
                loki_success, loki_success_value, merge_loki_query_response,
                merge_loki_query_stats, planned_block_bytes,
            },
            query_stats::loki_query_stats,
        },
        router::{
            compactor_router_with_delete_requests, flush_ingester_chunks, get_prepare_shutdown,
            log_level, log_level_post, loki_router_with_readiness, memberlist_status, ready,
            role_config, role_metrics, role_ring, role_services, set_prepare_shutdown,
            shutdown_ingester, unset_prepare_shutdown,
        },
    },
    querier::{
        aggregate::{
            metric_values::{
                MetricSampleState, VectorAggregationState, append_matching_log_row, eval_times,
                format_metric_value, rate_metric_value,
            },
            record_matching::{
                QueryRow, append_matching_hot_log_record, append_matching_hot_metric_record,
                append_matching_metric_row, is_deleted_log_entry, matching_loki_stream_entry,
                parse_decimal_sample_literal, parse_metric_sample_value,
                should_insert_unknown_detected_level, sort_loki_stream_values,
                structured_metadata_value,
            },
            sample_windows::{
                FormattedMetricSeries, METRIC_DECIMAL_SCALE, MetricSamples, MetricValue,
                MetricWindow, apply_absent_over_time, format_metric_samples,
                is_unwrapped_metric_query, merge_metric_samples, metric_samples_from_batches,
            },
        },
        analytics::{
            detected_fields::{
                collect_detected_fields, execute_index_volume_query, sample_time_bucket,
            },
            index_patterns::{
                execute_detected_field_values_query, execute_detected_fields_query,
                execute_detected_labels_query, execute_index_stats_query, execute_patterns_query,
            },
        },
        metadata::{
            execute_api_prom_label_names_query, execute_api_prom_series_query,
            execute_label_names_query, execute_label_values_query, execute_series_query,
            parse_series_params, series_data,
        },
        metric_eval::{
            binary_arithmetic::{
                apply_metric_binary_arithmetic_to_sample,
                apply_metric_binary_arithmetic_to_series_with_left_operand,
                apply_metric_binary_comparison_to_loki_result, matching_metric_binary_sample,
                metric_binary_sample_timestamps_match,
            },
            binary_sets::{
                apply_metric_binary_set_to_loki_result,
                apply_metric_scalar_arithmetic_to_loki_result,
                apply_metric_scalar_comparison_to_loki_result, default_metric_range_step,
                execute_http_metric_range_query, include_metric_group_labels,
                metric_scalar_arithmetic_value, metric_scalar_comparison_matches,
                metric_series_labels, metric_vector_group_modifier, metric_vector_matching_key,
            },
            execution::{
                execute_http_label_replace_metric_binary_expression,
                execute_http_metric_expression_query,
                execute_http_metric_vector_arithmetic_expression,
                execute_http_metric_vector_comparison_expression,
                execute_http_metric_vector_set_expression, execute_http_sort_vector_expression,
            },
            expression_parser::ScalarComparisonOp,
            expressions::{
                LabelReplaceMetricBinaryExpression, MetricVectorArithmeticExpression,
                MetricVectorComparisonExpression, MetricVectorSetExpression,
                ScalarVectorExpressionResult, SortVectorExpression,
                loki_instant_scalar_or_vector_response, loki_range_vector_response,
                parse_label_replace_expression, parse_label_replace_metric_binary_expression,
                parse_metric_vector_arithmetic_expression,
                parse_metric_vector_comparison_expression, parse_metric_vector_set_expression,
                parse_sort_vector_expression, scalar_vector_expression_result,
                strip_outer_parenthesized_expression,
            },
            http_queries::{execute_http_metric_instant_query, execute_http_stream_query},
            result_transforms::{
                apply_metric_binary_arithmetic_to_loki_result,
                execute_http_metric_binary_arithmetic_query,
                execute_http_metric_binary_comparison_query, execute_http_metric_binary_set_query,
                execute_http_metric_scalar_arithmetic_query,
                execute_http_metric_scalar_comparison_query,
                execute_http_scalar_vector_expression_result, metric_query_uses_approx_topk,
                metric_query_uses_count_values, retain_metric_binary_on_labels,
                sort_loki_vector_result,
            },
            scalar_samples::{
                ScalarSample, execute_http_metric_query, gcd_signed, parse_scalar_sample,
                resolved_range_step, validate_loki_query_range_resolution,
                validate_loki_range_query_range_limit, validate_loki_volume_query_range_limit,
                validate_query_length_limit, validate_query_range_limit,
            },
            validation::{
                VectorScalarExpressionParser, apply_label_join_to_loki_result,
                apply_label_replace_to_loki_result, reject_signed_vector_function_literal,
                scalar_vector_plain_parse_error, scalar_vector_query_is_vector,
            },
        },
        scan::{
            metric_scans::{
                append_matching_log_batches, collect_object_store_metric_log_batches,
                execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes,
                execute_metric_query_range_with_deletes,
                execute_metric_query_range_with_hot_tail_frontier_and_deletes,
                execute_metric_query_with_deletes,
                execute_metric_query_with_hot_tail_frontier_and_deletes,
                execute_tail_query_with_frontier_and_deletes,
            },
            object_store_scans::{
                execute_metric_query_range_from_object_store_with_hot_tail_frontier,
                execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
            },
            stream_scans::{
                QueryHotTail, StreamScanOptions,
                execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options,
                execute_stream_query_with_deletes,
                execute_stream_query_with_hot_tail_frontier_and_deletes, metric_scan_range,
            },
        },
        state::{
            object_store_support::{
                build_configured_querier_state, effective_object_store_prefix,
                querier_object_store_inputs, querier_object_store_prefix,
            },
            request_state::build_querier_state_with_object_store_prefix,
            types::{
                ColdObjectStoreState, DynamicIndexCache, DynamicIndexCacheKey, DynamicIndexSource,
                DynamicShardIndexCacheKey, DynamicShardRangesCacheKey, HotTailState,
                LokiRuleNamespaces, LokiRuleTenants, PrometheusAlertKey,
                PrometheusAlertRuntimeState, SharedLokiRules, SharedPrometheusAlertStates,
                merge_tenant_shard_indexes,
            },
        },
        tail::{hot_tail_snapshot, prepare_http_tail, send_tail_stream},
    },
    service::DeferredWalConsumerConnect,
    wal::{
        hot_tail::{poll_log_hot_tail_once_with_frontier, spawn_log_hot_tail_poller},
        pollers_and_records::{
            decode_native_kafka_log_record, has_native_kafka_log_headers,
            spawn_query_authorizer_connect, spawn_wal_hot_tail_connect_and_poll,
        },
        traits_and_kafka::{
            AllowAllIngestLimiter, AllowAllQueryAuthorizer, BrokerBackedIngestLimiter,
            BrokerBackedQueryAuthorizer, SwappableQueryAuthorizer, hot_tail_bucket_key,
        },
    },
};
