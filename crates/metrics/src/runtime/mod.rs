//! Metrics writer role configuration and serving coordinators.

use std::{
    ffi::OsStr,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use clap::{
    Arg, Command, Parser, ValueEnum,
    builder::{EnumValueParser, PossibleValue, TypedValueParser},
    error::{Error, ErrorKind},
};
use krabka_blockstore::{
    BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX,
    DEFAULT_MAX_BLOCKS_PER_JOB, DEFAULT_MAX_LEVEL, DEFAULT_TARGET_ROWS_PER_BLOCK,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::{
    ClientFrameMax, ClientSecurity, ConnectionDispatchQueueCapacity,
    DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_observability::{
    ConfigFileArgs, RoleReadiness,
    audit::AuditArgs,
    readiness_router,
    server_security::{ServerListener, ServerSecurity, ServerSecurityArgs, serve_router},
    topic_contract::{METRICS_TOPICS, require_topics},
    wal_client_security::WalClientSecurityArgs,
};
use krabka_units::{parse, prelude::*};
use object_store::ObjectStore;
use tokio::net::TcpListener;

use crate::{
    DEFAULT_MAX_RATE_BUCKETS, Limits, MetricsCompactorConfig, ObjectStoreCompactionIndexSink,
    OverridesProvider,
    distributor::{
        DistributorState, HA_TRACKER_TOPIC, KafkaHaElectionSink, KafkaSink,
        router as distributor_router, run_ha_election_consumer_loop,
    },
    metrics::ServiceMetrics,
    run_compactor_consumer_loop,
};

mod build_object_store;
mod cli;
mod compactor_loop;
mod compactor_policy_from_cli;
mod ingest_rate_bucket_cap;
mod load_runtime_overrides;
mod parse_compactor_target_rows;
mod parse_distributor_max_decompressed;
mod parse_ingest_rate_bucket_cap;
mod require_role_topics;
mod retired_role_message;
mod run;
mod run_block_builder;
mod run_compactor;
mod run_compactor_once;
mod run_distributor;
mod serve;
mod spawn_retention_sweeper;
mod target;
mod target_value_parser;
mod writer_config;

use build_object_store::build_object_store;
#[cfg_attr(test, mutants::skip)]
use compactor_loop::compactor_loop;
use compactor_policy_from_cli::compactor_policy_from_cli;
use ingest_rate_bucket_cap::IngestRateBucketCap;
use load_runtime_overrides::load_runtime_overrides;
use parse_compactor_target_rows::parse_compactor_target_rows;
use parse_distributor_max_decompressed::parse_distributor_max_decompressed;
use parse_ingest_rate_bucket_cap::parse_ingest_rate_bucket_cap;
use require_role_topics::require_role_topics;
use retired_role_message::retired_role_message;
use run_block_builder::run_block_builder;
#[cfg_attr(test, mutants::skip)]
use run_compactor::run_compactor;
use run_compactor_once::run_compactor_once;
use run_distributor::run_distributor;
#[cfg_attr(test, mutants::skip)]
use spawn_retention_sweeper::spawn_retention_sweeper;
use target::Target;
use target_value_parser::TargetValueParser;

use self::cli::Cli;
/// Options for one metrics writer role.
pub use self::cli::Cli as RuntimeConfig;
pub use self::{
    run::run,
    serve::{serve, serve_writer},
    target::Target as WriterTarget,
    writer_config::WriterConfig,
};

#[cfg(test)]
mod tests;
