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

use krabka_query_frontend::{AdmissionLimits, AdmissionLimitsOverride};
use krabka_units::{bytes, days, minutes, secs, serde_units};

use crate::{
    ByteSize, ByteSizeExt, Deserialize, Error, Serialize, ServiceConfig, ServiceConfigError,
    TenantId, Time, TimeExt, TimeRange,
};

pub(crate) mod non_negative_byte_size {
    use serde::{Deserializer, de::Error as _};

    use super::{ByteSize, ByteSizeExt, serde_units};

    /// Reads a size cap and rejects a negative one.
    ///
    /// The reasoning matches [`non_negative_time`](super::non_negative_time): zero
    /// is the one sentinel for "no cap", so a negative size is refused rather than
    /// read as a second one.
    ///
    /// # Errors
    ///
    /// If the value is not a human size string, or names a negative size.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        let value = serde_units::human::byte_size::deserialize(deserializer)?;
        if value < ByteSize::ZERO {
            return Err(D::Error::custom(
                "a size limit cannot be negative; use 0 to turn the limit off",
            ));
        }
        Ok(value)
    }
    }
pub(crate) mod non_negative_time {
    use serde::{Deserializer, de::Error as _};

    use super::{Time, TimeExt, serde_units};

    /// Reads a time cap and rejects a negative one.
    ///
    /// `human::time` accepts a signed magnitude, and every check in this crate
    /// applies only a cap greater than zero. A cap of `"-1s"` would therefore load
    /// cleanly and mean *unlimited*, but zero is the documented way to turn a cap
    /// off. A rejection at parse time keeps one sentinel.
    ///
    /// There is no `serialize` here. The write side needs no guard, because a
    /// [`Limits`](super::Limits) in memory has already been through this one, and
    /// `serde_units` writes the human form directly.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Time, D::Error> {
        let value = serde_units::human::time::deserialize(deserializer)?;
        if value < Time::ZERO {
            return Err(D::Error::custom(
                "a time limit cannot be negative; use 0 to turn the limit off",
            ));
        }
        Ok(value)
    }
    }
pub(crate) mod option_non_negative_byte_size {
    use serde::{Deserializer, de::Error as _};

    use super::{ByteSize, ByteSizeExt, serde_units};

    /// Reads the optional size cap and rejects a negative one.
    ///
    /// # Errors
    ///
    /// If the value is not a human size string, or names a negative size.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ByteSize>, D::Error> {
        let value = serde_units::human::option_byte_size::deserialize(deserializer)?;
        if value.is_some_and(|value| value < ByteSize::ZERO) {
            return Err(D::Error::custom(
                "a size limit cannot be negative; use 0 to turn the limit off",
            ));
        }
        Ok(value)
    }
    }
pub(crate) mod option_non_negative_time {
    use serde::{Deserializer, de::Error as _};

    use super::{Time, TimeExt, serde_units};

    /// Reads the optional time cap and rejects a negative one.
    ///
    /// The override path deserializes through `PartialLimits` and not `Limits`, so
    /// the guard exists on both. Without it a per-tenant override slips past the
    /// check its `Limits` twin makes. This module is deserialize-only, because
    /// nothing serializes `PartialLimits`.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Time>, D::Error> {
        let value = serde_units::human::option_time::deserialize(deserializer)?;
        if value.is_some_and(|value| value < Time::ZERO) {
            return Err(D::Error::custom(
                "a time limit cannot be negative; use 0 to turn the limit off",
            ));
        }
        Ok(value)
    }
    }
mod load_logs_limits_overrides_config;
mod overrides_provider;
pub(crate) use load_logs_limits_overrides_config::load_logs_limits_overrides_config;
pub use overrides_provider::OverridesProvider;

/// Moves a query's start forward to the oldest instant `max_query_lookback`
/// allows.
///
/// `Loki` clamps rather than refuses: "the request will not fail, but will be
/// modified to only query data within the allowed time range". A query that
/// starts before the boundary is answered from the boundary forward.
///
/// Krabka diverges in one case. When the whole window is older than the
/// boundary, `Loki` skips the query and answers with an empty body, and Krabka
/// instead runs the query over the single instant at the boundary. The answer
/// is empty for any data older than the boundary, which is every case this
/// branch exists for, and the response keeps the shape the engine gives every
/// other query.
pub(crate) fn clamp_query_lookback(limits: &Limits, range: TimeRange, now_ns: i64) -> TimeRange {
    if limits.max_query_lookback <= Time::ZERO {
        return range;
    }
    let oldest_ns = now_ns.saturating_sub(limits.max_query_lookback.nanos_i64());
    if range.start_ns >= oldest_ns {
        return range;
    }
    let end_ns = range.end_ns.max(oldest_ns);
    TimeRange::new(oldest_ns, end_ns).unwrap_or(range)
}

/// The one provider a logs service shares between its distributor and its
/// querier.
///
/// # Errors
/// [`ServiceConfigError::LimitsOverrides`] when
/// `--logs-limits-overrides-config` names a file that cannot be read or does
/// not parse.
pub(crate) fn limits_provider_for_config(
    config: &ServiceConfig,
) -> Result<Arc<OverridesProvider>, ServiceConfigError> {
    Ok(Arc::new(load_logs_limits_overrides_config(config)?))
}

/// One tenant's complete limit set.
///
/// The key names and the defaults are `Loki`'s, from `limits_config` in
/// `pkg/validation/limits.go` at the 3.7.7 tag the `loki_differential` suite
/// pins. Three fields are Krabka's own and say so. Zero turns admission caps
/// off; detection settings describe their own sentinels.
///
/// It is not `Eq`. Every dimensioned field stores `f64`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Shared cross-request query admission budgets.
    #[serde(default)]
    pub query_admission: AdmissionLimits,
    /// Largest accepted log line. `Loki` default: `256KB`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_line_size: ByteSize,

    /// Truncate oversized lines instead of rejecting their push.
    #[serde(default)]
    pub max_line_size_truncate: bool,

    /// Largest structured-metadata payload on one entry. `Loki` default: `64KB`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_structured_metadata_size: ByteSize,

    /// Most structured-metadata labels on one entry. `Loki` default: `128`.
    pub max_structured_metadata_entries_count: u64,

    /// Discover log levels during ingestion and add structured metadata.
    /// `Loki` default: enabled.
    #[serde(default = "default_discover_log_levels")]
    pub discover_log_levels: bool,

    /// Ordered field names used for log-level detection. An empty list uses
    /// `Loki`'s fourteen default names.
    #[serde(default = "default_log_level_fields")]
    pub log_level_fields: Vec<String>,

    /// JSON nesting levels searched for log levels. `Loki` default: `2`;
    /// zero and negative values allow unlimited depth.
    #[serde(default = "default_log_level_from_json_max_depth")]
    pub log_level_from_json_max_depth: i64,

    /// Per-tenant Loki OTLP resource-attribute actions.
    #[serde(default)]
    pub otlp_config: OtlpConfig,

    /// Most label names one stream may carry. `Loki` default: `15`.
    pub max_label_names_per_series: u64,

    /// Longest accepted label name. `Loki` default: `1024` bytes.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_label_name_length: ByteSize,

    /// Longest accepted label value. `Loki` default: `2048` bytes.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_label_value_length: ByteSize,

    /// Oldest accepted entry timestamp, measured back from now. `Loki`
    /// default: `7d`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub reject_old_samples_max_age: Time,

    /// Furthest accepted entry timestamp into the future. `Loki` default:
    /// `10m`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub creation_grace_period: Time,

    /// Largest accepted ingest request body.
    ///
    /// Krabka's own limit. `Loki` bounds a push body with its HTTP server
    /// settings and not with `limits_config`, so there is no upstream key to
    /// take. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_ingest_body: ByteSize,

    /// Widest `[start, end]` window a query may cover. `Loki` default: `721h`,
    /// which `Loki` writes as `30d1h`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub max_query_length: Time,

    /// How far back a query may reach. `Loki` default: `0s`, that is no cap.
    ///
    /// `Loki` clamps the query start to this instead of refusing the query, and
    /// so does Krabka.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub max_query_lookback: Time,

    /// Largest `limit` parameter a query may ask for. `Loki` default: `5000`.
    pub max_entries_limit_per_query: u64,

    /// Most series one query may match. `Loki` default: `500`.
    pub max_query_series: u64,

    /// Enables Loki's experimental multi-variant queries. Default: disabled.
    #[serde(default)]
    pub enable_multi_variant_queries: bool,

    /// Aggregations enabled for sharded execution. Loki's default is empty.
    #[serde(default)]
    pub shard_aggregations: Vec<String>,

    /// Largest summed size of the blocks one query plans to read. `Loki` calls
    /// this `max_query_bytes_read`, and its default is `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_query_read: ByteSize,

    /// Longest accepted `LogQL` query string, in bytes of source text.
    ///
    /// Krabka's own limit. It is deliberately not called `max_query_length`:
    /// `Loki` gives that key to the `[start, end]` window above, and one name
    /// for two meanings would mislead every operator who writes an overrides
    /// file. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "non_negative_byte_size::deserialize"
    )]
    pub max_query_string_bytes: ByteSize,

    /// A second, stricter cap on the `[start, end]` window a query may cover.
    ///
    /// Krabka's own limit, and a deliberate divergence. `Loki` has a key of
    /// this name, but it caps the `[range]` of a range selector, which Krabka
    /// does not limit. The effective window cap is the smaller of this and
    /// `max_query_length`. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub max_query_range: Time,

    /// How long a tenant's log blocks are kept. `Loki` default: `0s`.
    ///
    /// Zero keeps every block forever. It does **not** mean "delete
    /// everything": a tenant that configures nothing keeps every block it ever
    /// wrote, which is what `Loki` does with `retention_period: 0s`. Reading
    /// it the other way round would delete the whole history of every
    /// unconfigured tenant on the first sweep. A negative window is read the
    /// same way as zero.
    ///
    /// The compactor's retention sweep reads this window through
    /// [`krabka_blockstore::RetentionWindows`].
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "non_negative_time::deserialize"
    )]
    pub retention_period: Time,
}

// Loki 3.7.7 pkg/validation/limits.go:70–86,345–349. Explicitly empty
// overrides remain empty; the detector applies allowedLabelsForLevel's fallback.
fn default_discover_log_levels() -> bool {
    true
}

fn default_log_level_fields() -> Vec<String> {
    [
        "level",
        "LEVEL",
        "Level",
        "log.level",
        "severity",
        "SEVERITY",
        "Severity",
        "SeverityText",
        "lvl",
        "LVL",
        "Lvl",
        "severity_text",
        "Severity_Text",
        "SEVERITY_TEXT",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_log_level_from_json_max_depth() -> i64 {
    2
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            query_admission: AdmissionLimits::default(),
            // `_ = l.MaxLineSize.Set("256KB")`, and `KB` is 1000 bytes there.
            max_line_size: bytes(256_000),
            max_line_size_truncate: false,
            max_structured_metadata_size: bytes(64_000),
            max_structured_metadata_entries_count: 128,
            discover_log_levels: default_discover_log_levels(),
            log_level_fields: default_log_level_fields(),
            log_level_from_json_max_depth: default_log_level_from_json_max_depth(),
            otlp_config: OtlpConfig::default(),
            // `validation.max-label-names-per-series`
            max_label_names_per_series: 15,
            // `validation.max-length-label-name`
            max_label_name_length: bytes(1024),
            // `validation.max-length-label-value`
            max_label_value_length: bytes(2048),
            // `_ = l.RejectOldSamplesMaxAge.Set("7d")`
            reject_old_samples_max_age: days(7),
            // `_ = l.CreationGracePeriod.Set("10m")`
            creation_grace_period: minutes(10),
            // Krabka's own, and off until an operator asks for it.
            max_ingest_body: ByteSize::ZERO,
            // `_ = l.MaxQueryLength.Set("721h")`, which is 2_595_600 seconds.
            max_query_length: secs(2_595_600),
            // `_ = l.MaxQueryLookback.Set("0s")`
            max_query_lookback: Time::ZERO,
            // `validation.max-entries-limit`
            max_entries_limit_per_query: 5_000,
            // `querier.max-query-series`
            max_query_series: 500,
            enable_multi_variant_queries: false,
            shard_aggregations: Vec::new(),
            // `frontend.max-query-bytes-read`, which defaults to no cap.
            max_query_read: ByteSize::ZERO,
            // Krabka's own, and off until an operator asks for it.
            max_query_string_bytes: ByteSize::ZERO,
            // Krabka's own, and off until an operator asks for it.
            max_query_range: Time::ZERO,
            // `_ = l.RetentionPeriod.Set("0s")`
            retention_period: Time::ZERO,
        }
    }
}

impl Limits {
    /// A set with every limit turned off.
    ///
    /// [`distributor_router`](crate::distributor_router) builds one. It takes a
    /// WAL sink and nothing else, so there is no operator configuration for it
    /// to read a limit from, and a router that refused a push against a limit
    /// nobody set would be surprising.
    ///
    /// [`QuerierState::new`](crate::QuerierState::new) does not. It starts on
    /// [`Limits::default`], because `Loki` applies its own `max_query_length`
    /// to every read whether an operator configured one or not.
    #[must_use]
    pub fn unenforced() -> Self {
        Self {
            query_admission: AdmissionLimits::default(),
            max_line_size: ByteSize::ZERO,
            max_line_size_truncate: false,
            max_structured_metadata_size: ByteSize::ZERO,
            max_structured_metadata_entries_count: 0,
            discover_log_levels: default_discover_log_levels(),
            log_level_fields: default_log_level_fields(),
            log_level_from_json_max_depth: default_log_level_from_json_max_depth(),
            otlp_config: OtlpConfig::default(),
            max_label_names_per_series: 0,
            max_label_name_length: ByteSize::ZERO,
            max_label_value_length: ByteSize::ZERO,
            reject_old_samples_max_age: Time::ZERO,
            creation_grace_period: Time::ZERO,
            max_ingest_body: ByteSize::ZERO,
            max_query_length: Time::ZERO,
            max_query_lookback: Time::ZERO,
            max_entries_limit_per_query: 0,
            max_query_series: 0,
            enable_multi_variant_queries: false,
            shard_aggregations: Vec::new(),
            max_query_read: ByteSize::ZERO,
            max_query_string_bytes: ByteSize::ZERO,
            max_query_range: Time::ZERO,
            retention_period: Time::ZERO,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpAttributeAction {
    IndexLabel,
    #[default]
    StructuredMetadata,
    Drop,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpAttributesConfig {
    #[serde(default)]
    pub action: OtlpAttributeAction,
    #[serde(default)]
    pub attributes: Vec<String>,
    #[serde(default)]
    pub regex: Option<String>,
}

impl OtlpAttributesConfig {
    pub(crate) fn matches(&self, name: &str) -> bool {
        self.attributes.iter().any(|attribute| attribute == name)
            || self
                .regex
                .as_ref()
                .is_some_and(|pattern| regex::Regex::new(pattern).is_ok_and(|re| re.is_match(name)))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpResourceAttributesConfig {
    #[serde(default)]
    pub ignore_defaults: bool,
    #[serde(default)]
    pub attributes_config: Vec<OtlpAttributesConfig>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpConfig {
    #[serde(default)]
    pub resource_attributes: OtlpResourceAttributesConfig,
    #[serde(default)]
    pub scope_attributes: Vec<OtlpAttributesConfig>,
    #[serde(default)]
    pub log_attributes: Vec<OtlpAttributesConfig>,
}

/// Why a runtime overrides file did not load.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverridesError {
    #[error("failed to parse logs runtime overrides YAML: {0}")]
    Yaml(String),
    #[error("failed to read logs runtime overrides file {path}: {reason}")]
    Read { path: String, reason: String },
}

/// A sparse override: every field an operator left out keeps the value it
/// merges over.
///
/// `deny_unknown_fields` makes a misspelled key a load failure. A silently
/// ignored `max_line_sizes` would leave the operator believing a limit was set.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartialLimits {
    #[serde(default)]
    pub(crate) query_admission: AdmissionLimitsOverride,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_line_size: Option<ByteSize>,
    #[serde(default)]
    pub(crate) max_line_size_truncate: Option<bool>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_structured_metadata_size: Option<ByteSize>,
    #[serde(default)]
    pub(crate) max_structured_metadata_entries_count: Option<u64>,
    #[serde(default)]
    pub(crate) otlp_config: Option<OtlpConfig>,
    #[serde(default)]
    pub(crate) discover_log_levels: Option<bool>,
    #[serde(default)]
    pub(crate) log_level_fields: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) log_level_from_json_max_depth: Option<i64>,
    #[serde(default)]
    pub(crate) max_label_names_per_series: Option<u64>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_label_name_length: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_label_value_length: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) reject_old_samples_max_age: Option<Time>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) creation_grace_period: Option<Time>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_ingest_body: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_length: Option<Time>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_lookback: Option<Time>,
    #[serde(default)]
    pub(crate) max_entries_limit_per_query: Option<u64>,
    #[serde(default)]
    pub(crate) max_query_series: Option<u64>,
    #[serde(default)]
    pub(crate) enable_multi_variant_queries: Option<bool>,
    #[serde(default)]
    pub(crate) shard_aggregations: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_query_read: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_query_string_bytes: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_range: Option<Time>,
    #[serde(
        default,
        deserialize_with = "option_non_negative_time::deserialize"
    )]
    pub(crate) retention_period: Option<Time>,
}

/// The shape of the file `--logs-limits-overrides-config` names.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) defaults: PartialLimits,
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}

/// The process defaults the scalar CLI flags describe.
///
/// Each flag is optional, and an unset flag keeps `Loki`'s default. The two
/// timestamp-window flags always have a value, because `clap` gives them
/// `Loki`'s own default.
pub(crate) fn limits_for_config(config: &ServiceConfig) -> Limits {
    let defaults = Limits::default();
    Limits {
        reject_old_samples_max_age: config.reject_old_samples_max_age,
        creation_grace_period: config.creation_grace_period,
        max_ingest_body: config.max_ingest_body.unwrap_or(defaults.max_ingest_body),
        max_query_range: config.max_query_range.unwrap_or(defaults.max_query_range),
        max_query_series: config
            .max_query_series
            .map_or(defaults.max_query_series, |series| {
                u64::try_from(series).unwrap_or(u64::MAX)
            }),
        discover_log_levels: config.discover_log_levels,
        log_level_fields: config.log_level_fields.clone(),
        log_level_from_json_max_depth: config.log_level_from_json_max_depth,
        enable_multi_variant_queries: config.enable_multi_variant_queries,
        shard_aggregations: config.shard_aggregations.clone(),
        max_query_read: config.max_query_read.unwrap_or(defaults.max_query_read),
        max_query_string_bytes: config
            .max_query_string_bytes
            .unwrap_or(defaults.max_query_string_bytes),
        retention_period: config.retention_period.unwrap_or(defaults.retention_period),
        ..defaults
    }
}

/// Overlays a sparse override, or the file's `defaults` block, on top of
/// `base`.
///
/// An override is **fully trusted**. Any field the operator set replaces the
/// matching `base` value exactly, with no floor and no ceiling, and a value of
/// `0` turns that limit off for the tenant. This is `Loki`'s own
/// runtime-overrides rule: an operator-supplied override is authoritative.
pub(crate) fn merge_limits(base: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        query_admission: base.query_admission.merge(partial.query_admission),
        max_line_size: partial.max_line_size.unwrap_or(base.max_line_size),
        max_line_size_truncate: partial
            .max_line_size_truncate
            .unwrap_or(base.max_line_size_truncate),
        max_structured_metadata_size: partial
            .max_structured_metadata_size
            .unwrap_or(base.max_structured_metadata_size),
        max_structured_metadata_entries_count: partial
            .max_structured_metadata_entries_count
            .unwrap_or(base.max_structured_metadata_entries_count),
        discover_log_levels: partial
            .discover_log_levels
            .unwrap_or(base.discover_log_levels),
        log_level_fields: partial
            .log_level_fields
            .clone()
            .unwrap_or_else(|| base.log_level_fields.clone()),
        log_level_from_json_max_depth: partial
            .log_level_from_json_max_depth
            .unwrap_or(base.log_level_from_json_max_depth),
        otlp_config: partial
            .otlp_config
            .clone()
            .unwrap_or_else(|| base.otlp_config.clone()),
        max_label_names_per_series: partial
            .max_label_names_per_series
            .unwrap_or(base.max_label_names_per_series),
        max_label_name_length: partial
            .max_label_name_length
            .unwrap_or(base.max_label_name_length),
        max_label_value_length: partial
            .max_label_value_length
            .unwrap_or(base.max_label_value_length),
        reject_old_samples_max_age: partial
            .reject_old_samples_max_age
            .unwrap_or(base.reject_old_samples_max_age),
        creation_grace_period: partial
            .creation_grace_period
            .unwrap_or(base.creation_grace_period),
        max_ingest_body: partial.max_ingest_body.unwrap_or(base.max_ingest_body),
        max_query_length: partial.max_query_length.unwrap_or(base.max_query_length),
        max_query_lookback: partial
            .max_query_lookback
            .unwrap_or(base.max_query_lookback),
        max_entries_limit_per_query: partial
            .max_entries_limit_per_query
            .unwrap_or(base.max_entries_limit_per_query),
        max_query_series: partial.max_query_series.unwrap_or(base.max_query_series),
        enable_multi_variant_queries: partial
            .enable_multi_variant_queries
            .unwrap_or(base.enable_multi_variant_queries),
        shard_aggregations: partial
            .shard_aggregations
            .clone()
            .unwrap_or_else(|| base.shard_aggregations.clone()),
        max_query_read: partial.max_query_read.unwrap_or(base.max_query_read),
        max_query_string_bytes: partial
            .max_query_string_bytes
            .unwrap_or(base.max_query_string_bytes),
        max_query_range: partial.max_query_range.unwrap_or(base.max_query_range),
        retention_period: partial.retention_period.unwrap_or(base.retention_period),
    }
}
