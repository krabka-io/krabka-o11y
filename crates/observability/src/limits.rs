//! Per-tenant limits for the logs signal.
//!
//! Metrics, traces and profiles each resolve their limits per tenant through an
//! [`OverridesProvider`]. Logs does the same here. [`Limits`] holds one tenant's
//! complete set, and the provider merges a sparse per-tenant override on top of
//! the process defaults.
//!
//! Every key and every default comes from `Loki`'s `limits_config`, except the
//! three that [`Limits`] marks as Krabka's own. Zero turns a cap off, which is
//! `Loki`'s own sentinel.
//!
//! # The broker owns the ingest byte rate
//!
//! There is no per-tenant ingest rate limit in this file, and an operator must
//! not add one to the overrides file. The broker holds that quota:
//! `BrokerBackedIngestLimiter` reads the tenant's `producer_byte_rate` through
//! `describe_user_quotas` and enforces it per tenant. A second rate limit here
//! would fight the broker's, and the two would disagree about which tenant was
//! throttled. Set `producer_byte_rate` on the broker instead.
//!
//! # Where an operator sets these
//!
//! `--logs-limits-overrides-config` names a YAML file with a `defaults` block
//! and an `overrides` map keyed by tenant. The scalar CLI flags
//! (`--max-query-range` and its siblings) set the process defaults that the
//! file's `defaults` block, and then each tenant's entry, merge over.
//!
//! # Who reads these
//!
//! The distributor reads the ingest limits and the querier reads the query
//! limits. `retention_period` is the one limit the compactor reads: it is the
//! window its retention sweep expires a tenant's blocks against, through
//! [`krabka_blockstore::RetentionWindows`]. All three roles share one
//! provider, so they answer a tenant with the same numbers.

use std::{collections::HashMap, path::Path as FsPath, sync::Arc};

use krabka_units::{bytes, days, minutes, secs, serde_units};

use crate::{
    ByteSize, ByteSizeExt, Deserialize, Error, Serialize, ServiceConfig, ServiceConfigError,
    TenantId, Time, TimeExt, TimeRange,
};

pub(crate) mod non_negative_byte_size;
pub(crate) mod non_negative_time;
pub(crate) mod option_non_negative_byte_size;
pub(crate) mod option_non_negative_time;

mod clamp_query_lookback;
mod limits_for_config;
mod limits_provider_for_config;
mod limits_type;
mod load_logs_limits_overrides_config;
mod merge_limits;
mod overrides_error;
mod overrides_provider;
mod partial_limits;
mod runtime_file;

pub(crate) use clamp_query_lookback::clamp_query_lookback;
pub(crate) use limits_for_config::limits_for_config;
pub(crate) use limits_provider_for_config::limits_provider_for_config;
pub use limits_type::Limits;
pub(crate) use load_logs_limits_overrides_config::load_logs_limits_overrides_config;
pub(crate) use merge_limits::merge_limits;
pub use overrides_error::OverridesError;
pub use overrides_provider::OverridesProvider;
pub(crate) use partial_limits::PartialLimits;
pub(crate) use runtime_file::RuntimeFile;
