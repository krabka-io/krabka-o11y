pub(crate) mod prelude {
    pub(crate) use std::{
        cmp::Ordering,
        collections::{BTreeMap, BTreeSet},
        convert::Infallible,
        fmt::Write as _,
        future::{Future, IntoFuture, pending},
        io::{ErrorKind, Read as _},
        net::SocketAddr,
        num::NonZeroUsize,
        path::{Path as FsPath, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering as AtomicOrdering},
        },
        time::{Instant, SystemTime, UNIX_EPOCH},
    };

    pub(crate) use assert2::check;
    pub(crate) use async_trait::async_trait;
    pub(crate) use axum::{
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
    pub(crate) use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    pub(crate) use clap::{Parser, ValueEnum};
    pub(crate) use datafusion::{
        arrow::{
            array::{
                Array, ArrayRef, Float64Array, Int64Array, MapArray, StringArray,
                TimestampNanosecondArray, UInt64Array,
                builder::{MapBuilder, StringBuilder},
            },
            datatypes::{DataType, Field, Schema, TimeUnit},
            record_batch::RecordBatch,
        },
        error::DataFusionError,
        prelude::SessionContext,
    };
    pub(crate) use flate2::read::{DeflateDecoder, GzDecoder};
    pub(crate) use futures_util::{StreamExt as _, TryStreamExt as _};
    pub(crate) use krabka_blockstore::{
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
    pub(crate) use krabka_client_admin::{
        AclEntry, AclEntryFilter, AclOperation, AdminClient, AdminError, PatternType,
        PermissionType, ResourceType,
    };
    pub(crate) use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError};
    pub(crate) use krabka_client_producer::{
        Acks, Header as ProducerHeader, Producer, ProducerError, ProducerRecord,
    };
    pub(crate) use krabka_logql::{
        ComparisonOp, FieldFilter, FieldFilterExpression, FieldFilterLogicOp, FieldValue,
        LabelFormatValue, LabelSelectionMatcher, LabelSelectionSet, LineFilterOp,
        LogfmtParserConfig, MatchOp, MetricBinaryArithmetic, MetricBinaryComparison,
        MetricBinarySet, MetricBinarySetOp, MetricLabelJoin, MetricQuery, MetricScalarArithmetic,
        MetricScalarArithmeticOp, MetricScalarComparison, MetricVectorGroupModifier,
        MetricVectorMatching, ParseError, ParserStage, PipelineStage, PlanError, Quantile,
        RangeAggregation, StreamPlan, StreamQuery, UNWRAP_SAMPLE_VALUE_LABEL, UnwrapConversion,
        VectorAggregation, VectorAggregationOp, VectorGrouping,
        parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
        parse_metric_binary_set_query, parse_metric_label_join_query,
        parse_metric_label_replace_query, parse_metric_query, parse_metric_scalar_arithmetic_query,
        parse_metric_scalar_comparison_query, parse_query, plan_stream_query,
    };
    pub(crate) use krabka_units::{
        ByteRate, ByteSize, Time, bytes, bytes_per_sec,
        convert::{ByteRateExt as _, ByteSizeExt, StdDurationExt as _, TimeExt},
        days, hours, millis, minutes, secs,
    };
    pub(crate) use num_traits::{FromPrimitive as _, ToPrimitive as _};
    pub(crate) use object_store::{
        ObjectStore, ObjectStoreExt, local::LocalFileSystem, parse_url_opts,
        path::Path as ObjectPath,
    };
    pub(crate) use opentelemetry_proto::tonic::{
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
    pub(crate) use parquet::arrow::arrow_writer::ArrowWriter;
    pub(crate) use prost::Message as _;
    pub(crate) use regex::Regex;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use serde_json::{Value, json};
    pub(crate) use snap::raw::Decoder as SnappyDecoder;
    pub(crate) use thiserror::Error;
    pub(crate) use time::{OffsetDateTime, format_description::well_known::Rfc3339};
    pub(crate) use tokio::{
        net::TcpListener,
        task::JoinHandle,
        time::{Duration, sleep},
    };
    pub(crate) use tokio_util::sync::CancellationToken;
    pub(crate) use tracing::Instrument as _;
    pub(crate) use url::Url;

    pub(crate) use super::{
        acl_quota_and_buffers::*, alerts_and_params::*, cache_post_and_rules::*,
        compaction_and_query_limits::*, detected_fields_and_params::*, durations_and_tail::*,
        errors_labels_and_operators::*, formatting_and_errors::*, hot_metrics_and_metadata::*,
        hot_tail_frontier::*, ingest_and_operations::*, operators_and_alerts::*,
        patterns_and_prometheus_rules::*, query_limits_and_timestamps::*, rules_and_expressions::*,
        runtime_policies::*, scalar_rules_and_scans::*, scan_stats_and_samples::*,
        service_and_authorization::*, shard_index_cache::*, vector_binary_operations::*, *,
    };
    pub use crate::ids::{Offset, PartitionIndex};
    pub(crate) use crate::{
        compactor::{
            configuration::*, delete_materialization::*, frontier::*, object_store_support::*,
            runtime::*,
        },
        config::*,
        deletes::*,
        deletes_api::*,
        distributor::{
            ingest::*, loki_normalization::*, otlp_normalization::*, router::*, value_conversion::*,
        },
        error::{http_responses::*, query_errors::*},
        http::{
            handlers::{metadata_handlers::*, query_execution::*, request_types::*},
            params::{query_parsing::*, value_decoding::*},
            params_format::{
                aggregation_formatting::*, metric_formatting::*, request_formatting::*,
                stream_formatting::*,
            },
            response::{loki_responses::*, parquet_responses::*, query_stats::*},
            router::*,
        },
        metrics::ServiceMetrics,
        querier::{
            aggregate::{metric_values::*, record_matching::*, sample_windows::*},
            analytics::{detected_fields::*, index_patterns::*},
            metadata::*,
            metric_eval::{
                binary_arithmetic::*, binary_sets::*, execution::*, expression_parser::*,
                expressions::*, http_queries::*, result_transforms::*, scalar_samples::*,
                validation::*,
            },
            scan::{metric_scans::*, object_store_scans::*, stream_scans::*},
            state::{object_store_support::*, request_state::*, types::*},
            tail::*,
        },
        ruler::{
            api::{loki_api::*, prometheus_alerts::*, prometheus_rules::*},
            store::*,
        },
        service::*,
        service_runtime::*,
        wal::{hot_tail::*, pollers_and_records::*, traits_and_kafka::*},
    };
}

pub(crate) use prelude::*;

mod acl_quota_and_buffers;
mod alerts_and_params;
mod cache_post_and_rules;
mod compaction_and_query_limits;
mod detected_fields_and_params;
mod durations_and_tail;
mod errors_labels_and_operators;
mod formatting_and_errors;
mod hot_metrics_and_metadata;
mod hot_tail_frontier;
mod ingest_and_operations;
mod operators_and_alerts;
mod patterns_and_prometheus_rules;
mod query_limits_and_timestamps;
mod rules_and_expressions;
mod runtime_policies;
mod scalar_rules_and_scans;
mod scan_stats_and_samples;
mod service_and_authorization;
mod shard_index_cache;
mod vector_binary_operations;
