//! Role-selectable service skeleton for Krabka observability.

pub mod ids;
pub mod metrics;

use std::{
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
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, ValueEnum};
use datafusion::{
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
use flate2::read::{DeflateDecoder, GzDecoder};
use futures_util::{StreamExt as _, TryStreamExt as _};
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
    convert::{ByteRateExt as _, ByteSizeExt, StdDurationExt as _, TimeExt},
    days, hours, millis, minutes, secs,
};
use num_traits::{FromPrimitive as _, ToPrimitive as _};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, parse_url_opts, path::Path as ObjectPath,
};
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
use prost::Message as _;
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
use tracing::Instrument as _;
use url::Url;

use crate::metrics::ServiceMetrics;

// Role-oriented source files are included at crate scope to preserve the public API
// while allowing each subsystem to evolve independently.
include!("config.rs");
include!("service.rs");
include!("compactor/mod.rs");
include!("wal/mod.rs");
include!("distributor/mod.rs");
include!("querier/state.rs");
include!("service_runtime.rs");
include!("http/router.rs");
include!("deletes.rs");
include!("ruler/store.rs");
include!("deletes_api.rs");
include!("ruler/api.rs");
include!("http/handlers.rs");
include!("querier/metric_eval.rs");
include!("querier/analytics.rs");
include!("http/params_format.rs");
include!("querier/tail.rs");
include!("querier/metadata.rs");
include!("http/params.rs");
include!("querier/scan.rs");
include!("querier/aggregate.rs");
include!("http/response.rs");
include!("error.rs");
include!("tests.rs");
