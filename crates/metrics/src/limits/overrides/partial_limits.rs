use super::{ByteSize, Deserialize, Frequency, Time, serde_units};

/// A sparse override: every field an operator left out keeps the value it
/// merges over.
///
/// `deny_unknown_fields` makes a misspelled key a load failure. A silently
/// ignored `compactor_blocks_retention_period` would leave the operator
/// believing a retention window was set, and the symptom is a bucket that
/// grows forever with nothing to explain it. Mimir refuses an unknown limit
/// key the same way.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartialLimits {
    #[serde(default, with = "serde_units::human::option_frequency")]
    pub(crate) ingestion_rate: Option<Frequency>,
    #[serde(default)]
    pub(crate) ingestion_burst_size: Option<u64>,
    #[serde(default)]
    pub(crate) max_global_series_per_user: Option<u64>,
    #[serde(default)]
    pub(crate) max_series_per_request: Option<u64>,
    #[serde(default)]
    pub(crate) max_samples_per_series: Option<u64>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    pub(crate) max_label_name_length: Option<ByteSize>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    pub(crate) max_label_value_length: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) active_series_idle_timeout: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) otlp_delta_max_stale: Option<Time>,
    #[serde(default)]
    pub(crate) otlp_delta_max_streams: Option<u64>,
    #[serde(default)]
    pub(crate) max_samples_per_query: Option<u64>,
    #[serde(default)]
    pub(crate) max_fetched_series_per_query: Option<u64>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_lookback: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_length: Option<Time>,
    #[serde(default, with = "serde_units::human::option_time")]
    pub(crate) out_of_order_time_window: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) compactor_blocks_retention_period: Option<Time>,
}
